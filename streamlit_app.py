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
import re # ルーム売上の正規表現検索のため追加

# ロギング設定 (デバッグ用)
logging.basicConfig(level=logging.INFO)

# --- 定数設定 ---
# タイムチャージ請求書ページのURL
SR_TIME_CHARGE_URL = "https://www.showroom-live.com/organizer/show_rank_time_charge_hist_invoice_format" 
# プレミアムライブ請求書ページのURL (追加)
SR_PREMIUM_LIVE_URL = "https://www.showroom-live.com/organizer/paid_live_hist_invoice_format" 
# ルーム売上請求書ページのURL (追加)
SR_ROOM_SALES_URL = "https://www.showroom-live.com/organizer/point_hist_with_mixed_rate" 

# 処理するデータの種類とそれに対応するURL、ファイル名
DATA_TYPES = {
    "time_charge": {
        "label": "タイムチャージ売上",
        "url": SR_TIME_CHARGE_URL,
        # FTPパスの末尾に使用するファイル名部分
        "filename": "show_rank_time_charge_hist_invoice_format.csv",
        "type": "standard" 
    },
    "premium_live": {
        "label": "プレミアムライブ売上",
        "url": SR_PREMIUM_LIVE_URL,
        # FTPパスの末尾に使用するファイル名部分
        "filename": "paid_live_hist_invoice_format.csv",
        "type": "standard"
    },
    "room_sales": { # ルーム売上を追加
        "label": "ルーム売上",
        "url": SR_ROOM_SALES_URL,
        # FTPパスの末尾に使用するファイル名部分
        "filename": "point_hist_with_mixed_rate_csv_donwload_for_room.csv",
        "type": "room_sales"
    }
}

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
        # secretsで設定されたフルパスを使用することを推奨しますが、
        # ファイル名を動的に変更するため、ベースパスを設定。
        # 例: "/mksoul-pro.com/showroom/sales-app_v2/db/"
        "target_base_path": st.secrets["ftp"]["target_base_path"] 
    }
    # 既存のtarget_path設定を使用している場合は、ここでベースパスに変換
    if FTP_CONFIG["target_base_path"].endswith(".csv"):
        # ファイル名部分を削除して、パスの末尾に"/"を付けてベースパスとする
        base_path = '/'.join(FTP_CONFIG["target_base_path"].split('/')[:-1]) + '/'
        FTP_CONFIG["target_base_path"] = base_path
    
except KeyError as e:
    # secretsが存在しない場合はダミーを挿入してエラーを表示
    AUTH_COOKIE_STRING = "DUMMY"
    FTP_CONFIG = None
    if str(e) == "'target_base_path'":
         st.error(f"🚨 FTP設定が不完全です。`target_path`ではなく`target_base_path`を`.streamlit/secrets.toml`で設定してください。")
    else:
        st.error(f"🚨 認証またはFTP設定がされていません。`.streamlit/secrets.toml`を確認してください。不足: {e}")
    st.stop()


# --- ユーティリティ関数 ---

def get_target_months():
    """2023年10月以降の月リストを 'YYYY年MM月分' 形式で生成し、正確なUNIXタイムスタンプを計算する"""
    START_YEAR = 2023
    START_MONTH = 10
    
    today = datetime.now(JST)
    months = []
    
    # 処理は現在月から開始し、過去へ遡る
    current_year = today.year
    current_month = today.month
    
    while True:
        # 現在処理している月が開始月より前ではないかチェック
        if current_year < START_YEAR or (current_year == START_YEAR and current_month < START_MONTH):
            break # 2023年10月より前の月になったらループを終了

        month_str = f"{current_year}年{current_month:02d}月分"
        
        try:
            # 1. タイムゾーン情報のないdatetimeオブジェクトを生成
            # 月の初日を設定
            dt_naive = datetime(current_year, current_month, 1, 0, 0, 0)
            
            # 2. JSTでローカライズ
            # is_dst=None を使用し、曖昧さの解決を強制し、安全なローカライズを保証
            dt_obj_jst = JST.localize(dt_naive, is_dst=None)
            
            # 3. UNIXタイムスタンプ（UTC基準）に変換
            timestamp = int(dt_obj_jst.timestamp()) 
            
            months.append((month_str, timestamp))
        except Exception as e:
            logging.error(f"日付計算エラー ({month_str}): {e}")
            
        # 次の月（前の月）へ移動
        if current_month == 1:
            current_month = 12
            current_year -= 1
        else:
            current_month -= 1
            
    # monthsリストは既に最新の月が先頭に来るように降順で作成されている
    return months


def create_authenticated_session(cookie_string):
    """手動で取得したCookie文字列から認証済みRequestsセッションを構築する"""
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

def fetch_and_process_data(timestamp, cookie_string, sr_url, data_type_key):
    """
    指定されたタイムスタンプに基づいてSHOWROOMからデータを取得し、BeautifulSoupで整形する
    """
    st.info(f"データ取得中... URL: {sr_url}, タイムスタンプ: {timestamp}")
    session = create_authenticated_session(cookie_string)
    if not session:
        return None
    
    try:
        # 1. データ取得
        url = f"{sr_url}?from={timestamp}" 
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
            'Referer': sr_url
        }
        
        response = session.get(url, headers=headers, timeout=30)
        response.raise_for_status() # HTTPエラーが発生した場合に例外を発生させる
        
        # 2. HTMLからのデータ抽出
        soup = BeautifulSoup(response.text, 'html5lib') 
        
        # 売上データが格納されているテーブルをクラス名で特定 (table-type-02)
        table = soup.find('table', class_='table-type-02') 
        
        if not table:
            if "ログイン" in response.text or "会員登録" in response.text:
                st.error("🚨 認証切れです。Cookieが古いか無効になっています。")
                return None
            st.warning("HTMLから売上データテーブル (`table-type-02`) を検出できませんでした。ページ構造が変更されたか、データがまだ生成されていません。")
            
        
        # 3. データをBeautifulSoupで抽出 (ライバー個別のデータ)
        table_data = []
        # tableがNoneでない場合にのみ行を抽出
        if table:
            rows = table.find_all('tr')
            
            # ヘッダー行をスキップし、データ行のみを処理 (rows[1:]から開始)
            for row in rows[1:]: 
                td_tags = row.find_all('td')
                
                # --- 抽出ロジック（タイムチャージ/プレミアムライブ/ルーム売上で共通） ---
                # HTML構造: [0: ルームID, 1: ルームURL, 2: ルーム名, 3: 分配額, 4: アカウントID]
                if len(td_tags) >= 5:
                    # 必要なデータ: 3番目のtd (分配額) と 4番目のtd (アカウントID)
                    # 分配額はカンマを除去
                    amount_str = td_tags[3].text.strip().replace(',', '') 
                    account_id = td_tags[4].text.strip()
                    
                    # 分配額が数値であることを確認（合計行などを除外）
                    if amount_str.isnumeric():
                         table_data.append({
                            # CSVの列順に合わせて名前を付ける
                            '分配額': amount_str, 
                            'アカウントID': account_id
                        })
        
        # 4. DataFrameに変換し、整形 (ロジックの分岐)
        
        # 4-A. ルーム売上の特殊ロジック
        if data_type_key == "room_sales":
            
            # 1. 支払い金額（税抜）の抽出 (1行目1列目の値)
            total_amount_tag = soup.find('p', class_='fs-b4 bg-light-gray p-b3 mb-b2 link-light-green')
            total_amount_str = '0' # デフォルト値を '0' に設定
            if total_amount_tag:
                # <span>タグを検索して、支払い金額（税抜）を抽出
                # '支払い金額（税抜）: <span class="fw-b"> 1,182,445円</span><br>'
                                
                # 支払い金額（税抜）の行を抽出
                match = re.search(r'支払い金額（税抜）:\s*<span[^>]*>\s*([\d,]+)円', str(total_amount_tag))
                
                if match:
                    # カンマと '円' を除去
                    total_amount_str = match.group(1).replace(',', '') 
                else:
                    st.warning("⚠️ HTMLから「支払い金額（税抜）」の値を抽出できませんでした。分配額を「0」として処理を続行します。")
                    
            # 2. 1行目のヘッダーデータを作成 (合計値 + MKsoul)
            header_data = [{
                '分配額': total_amount_str,
                'アカウントID': 'MKsoul'
            }]
            
            # 3. ライバー個別のデータと結合
            header_df = pd.DataFrame(header_data)
            
            if table_data:
                # ライバーデータが存在する場合、header_dfの後ろに連結
                driver_df = pd.DataFrame(table_data)
                df_cleaned = pd.concat([header_df, driver_df], ignore_index=True)
                st.success(f"テーブルデータ ({len(driver_df)}件) の抽出と合計値 ({total_amount_str}) の設定が完了しました。")
            else:
                # ライバーデータが存在しない場合、header_df（1行）のみ (ゼロ件時も '0,MKsoul,更新日時' になる)
                df_cleaned = header_df
                st.warning(f"⚠️ ライバー個別のデータ行を抽出できませんでした。合計値 ({total_amount_str}) と MKsoul のみを含む1行データとして処理を続行します。")


        # 4-B. タイムチャージ/プレミアムライブの既存ロジック (0件時のダミーデータ生成)
        else: # time_charge or premium_live
            if not table_data:
                st.warning("⚠️ テーブルから有効なデータ行を抽出できませんでした。分配額=0、アカウントID=dummyを含む1行データとして処理を続行します。")
                
                # ゼロ件データ用のDataFrameを作成。分配額=0、アカウントID=dummyを設定
                df_cleaned = pd.DataFrame([{
                    '分配額': '0',       # 分配額: 0 (文字列)
                    'アカウントID': 'dummy' # アカウントID: dummy
                }])
                
            else:
                st.success(f"テーブルデータ ({len(table_data)}件) の抽出が完了しました。")
                df_cleaned = pd.DataFrame(table_data)

        # 5. 特殊なCSV形式の作成（共通ロジック）
        
        now_jst = datetime.now(JST)
        update_time_str = now_jst.strftime('%Y/%m/%d %H:%M')
        
        # --- CSV形式の再修正ロジック ---
        # 構造: [分配額], [アカウントID], [更新日時] の3列
        # 更新日時は1行目のみに記載し、2行目以降は空にする
        
        # 1. データを格納するための新しいDataFrameを準備
        final_df = pd.DataFrame({
            '分配額': df_cleaned['分配額'],
            'アカウントID': df_cleaned['アカウントID'],
            '更新日時': '' # デフォルトで空文字列
        })
        
        # 2. 最初のデータ行（インデックス0）の「更新日時」列にのみ、現在時刻を設定
        if not final_df.empty:
            final_df.loc[0, '更新日時'] = update_time_str
        
        # CSVデータとして一時的にメモリに書き出す
        csv_buffer = io.StringIO()
        # UTF-8、ヘッダーなし、インデックスなし
        final_df.to_csv(csv_buffer, index=False, header=False, encoding='utf-8')
        
        st.success("データの整形が完了しました。")
        # プレビュー表示（ヘッダーなし、インデックスなしのCSV文字列全体）
        st.code(csv_buffer.getvalue(), language='text') 
        
        return csv_buffer
        
    except requests.exceptions.HTTPError as e:
        st.error(f"HTTPエラーが発生しました: {e.response.status_code}. 認証Cookieが無効になっている可能性があります。")
        return None
    except Exception as e:
        st.error(f"予期せぬエラーが発生しました: {e}")
        logging.error("データ取得・整形エラー", exc_info=True)
        return None

def upload_file_ftp(csv_buffer, ftp_config, full_target_path):
    """
    FTPサーバーに整形済みCSVファイルをアップロードする 
    """
    st.info(f"FTPサーバー ({ftp_config['host']}) に接続し、ファイルをアップロードします... (パス: {full_target_path})")
    
    try:
        csv_buffer.seek(0)
        # FTP接続
        with FTP(ftp_config['host'], ftp_config['user'], ftp_config['password']) as ftp:
            # バイナリデータとしてアップロード
            csv_bytes = csv_buffer.getvalue().encode('utf-8')
            
            ftp.storbinary(f'STOR {full_target_path}', io.BytesIO(csv_bytes))
            
            st.success(f"✅ ファイルのアップロードが完了しました！")
            st.markdown(f"**アップロード先:** `{ftp_config['host']}:{full_target_path}`")
            
    except Exception as e:
        st.error(f"FTPアップロード中にエラーが発生しました。設定（ホスト名、ユーザー、パスワード、パス）を確認してください: {e}")
        logging.error("FTPエラー", exc_info=True)
        return False
        
    return True


def process_data_type(data_type_key, selected_timestamp, auth_cookie_string, ftp_config):
    """
    指定されたデータタイプ（タイムチャージ、プレミアムライブ、またはルーム売上）の処理を実行する
    """
    data_info = DATA_TYPES[data_type_key]
    data_label = data_info["label"]
    sr_url = data_info["url"]
    filename = data_info["filename"]
    
    # FTPアップロード先のフルパスを動的に生成
    full_target_path = ftp_config["target_base_path"] + filename
    
    st.subheader(f"🔄 **{data_label}** の処理を開始します")
    
    # 1. データ取得と整形 (data_type_keyを渡す)
    csv_buffer = fetch_and_process_data(selected_timestamp, auth_cookie_string, sr_url, data_type_key)
    
    if csv_buffer:
        # 2. FTPアップロード
        if ftp_config:
            upload_file_ftp(csv_buffer, ftp_config, full_target_path)
        else:
            st.error("FTP設定が読み込まれていないため、アップロードはスキップされました。")
    else:
        # fetch_and_process_dataがエラーなどでNoneを返した場合のみ実行される
        st.error(f"{data_label}のデータ取得・整形に失敗したため、アップロードはスキップされました。")
        
    st.markdown("---")
    
# --- Streamlit UI ---

def main():
    st.set_page_config(page_title="SHOWROOM売上データ アップロードツール", layout="wide")
    st.title("ライバー売上データ 自動アップロードツール (タイムチャージ / プレミアムライブ / ルーム売上)")
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
    if st.button("🚀 タイムチャージ売上 / プレミアムライブ売上 / ルーム売上の全てを取得・FTPアップロードを実行", type="primary"):
        with st.spinner(f"処理中: {selected_label}のデータを取得しています..."):
            
            # --- タイムチャージ売上処理 ---
            process_data_type("time_charge", selected_timestamp, AUTH_COOKIE_STRING, FTP_CONFIG)
            
            # --- プレミアムライブ売上処理 ---
            process_data_type("premium_live", selected_timestamp, AUTH_COOKIE_STRING, FTP_CONFIG)

            # --- ルーム売上処理 --- (追加)
            process_data_type("room_sales", selected_timestamp, AUTH_COOKIE_STRING, FTP_CONFIG)

        st.balloons()
        st.success("🎉 **全ての処理が完了しました！**")


if __name__ == "__main__":
    main()