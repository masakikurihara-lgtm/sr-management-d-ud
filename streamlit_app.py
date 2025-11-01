import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import calendar
from ftplib import FTP
import io
import pytz
import logging
from bs4 import BeautifulSoup # HTML解析のためbs4をインポート

# ロギング設定 (デバッグ用)
logging.basicConfig(level=logging.INFO)

# --- 定数設定 ---
# タイムチャージ請求書ページのURL
SR_BASE_URL = "https://www.showroom-live.com/organizer/show_rank_time_charge_hist_invoice_format" 
# アップロード先ファイル名
TARGET_FILENAME = "show_rank_time_charge_hist_invoice_format.csv"
# 日本のタイムゾーン
JST = pytz.timezone('Asia/Tokyo')

# --- 設定ロードと認証 ---
try:
    # オーガナイザーCookieを取得
    AUTH_COOKIE_STRING = st.secrets["showroom"]["auth_cookie_string"]
    # FTP設定
    FTP_CONFIG = {
        "host": st.secrets["ftp"]["host"],
        "user": st.secrets["ftp"]["user"],
        "password": st.secrets["ftp"]["password"],
        # secretsで設定されたフルパスを使用することを推奨しますが、暫定的に決め打ち
        "target_path": "/showroom/sales-app_v2/db/show_rank_time_charge_hist_invoice_format.csv" 
    }
except KeyError as e:
    # secretsが存在しない場合はダミーを挿入してエラーを表示
    AUTH_COOKIE_STRING = "DUMMY"
    FTP_CONFIG = None
    st.error(f"🚨 認証またはFTP設定がされていません。`.streamlit/secrets.toml`を確認してください。不足: {e}")
    st.stop()


# --- ユーティリティ関数 ---

def get_target_months(years=2):
    """過去N年間の月リストを 'YYYY年MM月分' 形式で生成し、正確なUNIXタイムスタンプを計算する"""
    today = datetime.now(JST)
    months = []
    
    # 選択肢の表示を当月含む過去2年分程度に限定
    for y in range(today.year, today.year - years, -1): # 降順で年を処理
        start_m = 12 if y < today.year else today.month
        
        for m in range(start_m, 0, -1): # 月を降順で処理
            
            # 今後の月は除外 (ただし、既に過去の月しか見ていないため実質不要だが念のため)
            if y == today.year and m > today.month:
                continue 
            
            month_str = f"{y}年{m:02d}月分"
            
            try:
                # 1. タイムゾーン情報のないdatetimeオブジェクトを生成
                dt_naive = datetime(y, m, 1, 0, 0, 0)
                
                # 2. JSTでローカライズ
                # is_dst=None を使用し、曖昧さの解決を強制し、安全なローカライズを保証
                dt_obj_jst = JST.localize(dt_naive, is_dst=None)
                
                # 3. UNIXタイムスタンプ（UTC基準）に変換
                timestamp = int(dt_obj_jst.timestamp()) 
                
                # --- ご指摘のあった正確な値の検証 ---
                if y == 2025 and m == 10:
                    expected_ts = 1759244400
                    if timestamp != expected_ts:
                         logging.error(f"FATAL: 2025年10月のTSが不一致: {timestamp}. 期待値: {expected_ts}")
                
                if y == 2025 and m == 9:
                    expected_ts = 1756652400
                    if timestamp != expected_ts:
                         logging.error(f"FATAL: 2025年9月のTSが不一致: {timestamp}. 期待値: {expected_ts}")
                # ==========================================

                months.append((month_str, timestamp))
            except Exception as e:
                logging.error(f"日付計算エラー ({month_str}): {e}")
                continue
                
    # 最新の月が上に来るようにする（既に降順になっているが念のため）
    return months


def create_authenticated_session(cookie_string):
    """手動で取得したCookie文字列から認証済みRequestsセッションを構築する (参照コードと同じロジック)"""
    st.info("認証セッションを構築します...")
    session = requests.Session()
    try:
        cookies_dict = {}
        for item in cookie_string.split(';'):
            item = item.strip()
            if '=' in item:
                name, value = item.split('=', 1)
                cookies_dict[name.strip()] = value.strip()
        cookies_dict['i18n_redirected'] = 'ja'
        session.cookies.update(cookies_dict)
        
        if not cookies_dict:
            st.error("🚨 有効な認証セッションを解析できませんでした。")
            return None
            
        return session
    except Exception as e:
        st.error(f"認証セッションを解析中にエラーが発生しました: {e}")
        return None

def fetch_and_process_data(timestamp, cookie_string):
    """
    指定されたタイムスタンプに基づいてSHOWROOMからデータを取得し、BeautifulSoupで整形する
    """
    st.info(f"データ取得中... タイムスタンプ: {timestamp}")
    session = create_authenticated_session(cookie_string)
    if not session:
        return None
    
    try:
        # 1. データ取得
        url = f"{SR_BASE_URL}?from={timestamp}" 
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
            'Referer': SR_BASE_URL # Refererはあればより安全
        }
        
        response = session.get(url, headers=headers, timeout=30)
        response.raise_for_status() # HTTPエラーが発生した場合に例外を発生させる
        
        # 2. HTMLからのデータ抽出 (BeautifulSoup + html5libパーサーを使用)
        # これによりlxmlのインストールエラーを完全に回避します
        soup = BeautifulSoup(response.text, 'html5lib') 
        
        # 売上データが格納されているテーブルをクラス名で特定 (table-type-02)
        table = soup.find('table', class_='table-type-02') 
        
        if not table:
            if "ログイン" in response.text or "会員登録" in response.text:
                st.error("🚨 認証切れです。Cookieが古いか無効になっています。")
                return None
            st.error("HTMLから売上データテーブル (`table-type-02`) を検出できませんでした。ページ構造が変更されたか、データがまだ生成されていません。")
            return None
        
        # 3. データをBeautifulSoupで抽出
        table_data = []
        rows = table.find_all('tr')
        
        # ヘッダー行をスキップし、データ行のみを処理
        for row in rows[1:]: 
            td_tags = row.find_all('td')
            
            # tdタグが5つある行のみを処理 (ルームID, ルームURL, ルーム名, 分配額, アカウントID)
            if len(td_tags) == 5:
                # 必要なデータ: 3番目のtd (分配額) と 4番目のtd (アカウントID)
                amount = td_tags[3].text.strip().replace(',', '') # 分配額からカンマを除去
                account_id = td_tags[4].text.strip() # アカウントID
                
                # 分配額が数値であることを確認（合計行などを除外）
                if amount.isnumeric():
                     table_data.append({
                        '分配額': amount,
                        'アカウントID': account_id
                    })
        
        if not table_data:
            st.warning("⚠️ テーブルから有効なデータ行を抽出できませんでした。")
            return None

        # 4. DataFrameに変換し、整形
        df_cleaned = pd.DataFrame(table_data)
        st.success(f"テーブルデータ ({len(df_cleaned)}件) の抽出が完了しました。")

        # 5. 特殊なヘッダー行の作成 (CSV形式に合わせる)
        
        now_jst = datetime.now(JST)
        update_time_str = now_jst.strftime('%Y/%m/%d %H:%M')
        
        # CSVのヘッダー行: [分配額, アカウントID, 更新日時(3列目のみ)]
        header_row = pd.DataFrame([['', '', update_time_str]], columns=['分配額', 'アカウントID', '更新日時'])
        
        # データ行を再構成 (3列目を空に設定)
        df_data = pd.DataFrame({
            '分配額': df_cleaned['分配額'],
            'アカウントID': df_cleaned['アカウントID'],
            '更新日時': '' 
        })
        
        # ヘッダー行とデータ行を結合
        final_df = pd.concat([header_row, df_data], ignore_index=True)
        
        # CSVデータとして一時的にメモリに書き出す
        csv_buffer = io.StringIO()
        # UTF-8、ヘッダーなし、インデックスなし
        final_df.to_csv(csv_buffer, index=False, header=False, encoding='utf-8')
        
        st.success("データの整形が完了しました。")
        st.code('\n'.join(csv_buffer.getvalue().split('\n')[:5]), language='text') # 整形後のCSVプレビュー
        
        return csv_buffer
        
    except requests.exceptions.HTTPError as e:
        st.error(f"HTTPエラーが発生しました: {e.response.status_code}. 認証Cookieが無効になっている可能性があります。")
        return None
    except Exception as e:
        st.error(f"予期せぬエラーが発生しました: {e}")
        logging.error("データ取得・整形エラー", exc_info=True)
        return None

def upload_file_ftp(csv_buffer, ftp_config):
    """
    FTPサーバーに整形済みCSVファイルをアップロードする
    """
    st.info(f"FTPサーバー ({ftp_config['host']}) に接続し、ファイルをアップロードします...")
    
    try:
        csv_buffer.seek(0)
        # FTP接続
        with FTP(ftp_config['host'], ftp_config['user'], ftp_config['password']) as ftp:
            # サーバーへアップロード
            csv_bytes = csv_buffer.getvalue().encode('utf-8')
            
            # バイナリデータとしてアップロード
            ftp.storbinary(f'STOR {ftp_config["target_path"]}', io.BytesIO(csv_bytes))
            
            st.success(f"✅ ファイルのアップロードが完了しました！")
            st.markdown(f"**アップロード先:** `{ftp_config['host']}:{ftp_config['target_path']}`")
            
    except Exception as e:
        st.error(f"FTPアップロード中にエラーが発生しました。設定（ホスト名、ユーザー、パスワード、パス）を確認してください: {e}")
        logging.error("FTPエラー", exc_info=True)
        return False
        
    return True

# --- Streamlit UI ---

def main():
    st.set_page_config(page_title="SHOWROOM売上データ アップロードツール", layout="wide")
    st.title("ライバー売上データ 自動アップロードツール (タイムチャージ)")
    st.markdown("---")

    # 2. 月選択プルダウンの作成
    month_options = get_target_months()
    month_labels = [label for label, _ in month_options]
    
    st.header("1. 対象月選択")
    
    selected_label = st.selectbox(
        "処理対象の配信月を選択してください:",
        options=month_labels,
        index=0 # デフォルトで最新の月を選択
    )
    
    selected_timestamp = next((ts for label, ts in month_options if label == selected_label), None)

    if selected_timestamp is None:
        st.warning("有効な月が選択されていません。")
        return
        
    st.info(f"選択された月: **{selected_label}** (UNIXタイムスタンプ: {selected_timestamp})")
    
    st.header("2. データ取得とアップロードの実行")
    
    # 3. 実行ボタン
    if st.button("🚀 データ取得・整形・FTPアップロードを実行", type="primary"):
        with st.spinner(f"処理中: {selected_label}のデータを取得しています..."):
            
            # 1. データ取得と整形
            csv_buffer = fetch_and_process_data(selected_timestamp, AUTH_COOKIE_STRING)
            
            if csv_buffer:
                # 2. FTPアップロード
                if FTP_CONFIG:
                    upload_file_ftp(csv_buffer, FTP_CONFIG)
                else:
                    st.error("FTP設定が読み込まれていないため、アップロードはスキップされました。")
            else:
                st.error("データ取得・整形に失敗したため、アップロードはスキップされました。")

if __name__ == "__main__":
    # FTPライブラリのインポートはmainの外側に移動済み
    main()
