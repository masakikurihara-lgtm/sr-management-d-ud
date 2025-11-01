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
        # FTPサーバー上の物理パスを設定。
        # 成功実績のあるパス構造に合わせて、ホスト名をパスの起点に含めます。
        "target_path": "/mksoul-pro.com/showroom/sales-app_v2/db/show_rank_time_charge_hist_invoice_format.csv" 
    }
except KeyError as e:
    # secretsが存在しない場合はダミーを挿入してエラーを表示
    AUTH_COOKIE_STRING = "DUMMY"
    FTP_CONFIG = None
    st.error(f"🚨 認証またはFTP設定がされていません。`.streamlit/secrets.toml`を確認してください。不足: {e}")
    st.stop()


# --- ユーティリティ関数 ---

def get_target_months(years=2):
    """過去指定年数分の年月 (YYYYMM) のリストを返す"""
    today = datetime.now(JST).date()
    target_months = []
    
    # 24ヶ月以上遡らないように上限を設定
    max_months_to_check = years * 12 + 1 

    for i in range(max_months_to_check):
        # 現在の月から i ヶ月前の日付を計算
        month_ago = today - timedelta(days=30 * i)
        
        # 取得対象の年月を YYYYMM 形式で格納
        target_months.append(month_ago.strftime("%Y%m"))
        
        # 取得する年月のリストは重複を排除
        target_months = sorted(list(set(target_months)), reverse=True)
        
        # 取得対象が指定された年数に達したら終了
        if len(target_months) >= years * 12:
             break

    # 常に最新の12か月*2年(24ヶ月)分を返す
    return target_months[:years*12]


@st.cache_data(ttl=600)
def fetch_month_data(month_str, auth_cookie_string):
    """特定の月 (YYYYMM) のデータをSHOWROOMのページから取得・解析する"""
    # URLに年月 (YYYYMM) を含める
    url = f"{SR_BASE_URL}?month={month_str}"
    
    headers = {
        "Cookie": auth_cookie_string,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # 200以外のステータスコードは例外を発生させる
    except requests.exceptions.HTTPError as e:
        # 404 Not Found などのエラー処理
        logging.error(f"HTTP Error for {month_str}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        # その他のリクエストエラー
        logging.error(f"Request Error for {month_str}: {e}")
        return None

    # HTML解析
    soup = BeautifulSoup(response.content, 'html.parser')

    # テーブル要素を検索 (ここではIDやクラスではなく、構造で探す)
    # 具体的なテーブル構造が不明なため、ここでは一旦ページ内の全てのテーブルを探す
    tables = soup.find_all('table')
    
    if not tables:
        # データがない場合や、ログインしていない場合（テーブルが存在しない）
        # ログイン画面にリダイレクトされているか確認 (今回はCookie認証なので不要な可能性あり)
        logging.warning(f"No tables found for month {month_str}. Check login status or page structure.")
        return None

    # 適切なテーブルを特定する必要がある。今回は最初のテーブルを試す
    try:
        # PandasでHTMLからテーブルを直接読み込む
        df_list = pd.read_html(response.text, header=0, encoding='utf-8')
        # 複数のテーブルが見つかる可能性があるので、最もデータが多いテーブルを選ぶなど調整が必要だが、
        # ここでは一旦、最も適切なテーブル（最初のテーブルなど）を試す
        
        # タイムチャージの請求書フォーマットは一つの主要なテーブルを持つと仮定し、
        # カラム名でヘッダーが適切に検出されたものを探す
        
        target_df = None
        for df in df_list:
            # 必要なカラム名の一部 ('ルーム名', '時間帯', '時間(h)') などが含まれているかチェック
            if any(col in df.columns for col in ['ルーム名', '時間帯', '時間(h)']) or len(df.columns) > 5:
                target_df = df
                break
        
        if target_df is None:
            logging.warning(f"Could not find the target table in {month_str}.")
            return None

        # データフレームをクリーンアップ
        return clean_data(target_df, month_str)

    except ValueError as e:
        # テーブルが見つからない、または解析できない場合
        logging.warning(f"Failed to parse HTML tables for {month_str}: {e}")
        return None


def clean_data(df, month_str):
    """取得したデータフレームを整形・クリーニングする"""
    
    # 1. 不要なフッター行の削除 (例: '合計'を含む行)
    # NaNが多い行や、特定の集計行を削除する処理をここに追加
    # カラムが標準化されていないため、今回はカラム数が多い行のみを対象とする
    # 1行目（ヘッダー行）をスキップした後の行を対象とする
    if '合計' in df.to_string():
        df = df[~df.apply(lambda row: row.astype(str).str.contains('合計').any(), axis=1)]
    
    # NaNが多い（空の行）を削除
    df.dropna(how='all', inplace=True)

    # 2. カラム名の標準化 (SHOWROOMのページ構造に依存)
    # ページを解析して、カラム名を特定し、標準化する
    
    # ページによってカラム名が変動する可能性があるため、確実な識別子を見つける
    
    # 暫定的なカラムマッピング（実際のデータに基づいて調整が必要）
    column_mapping = {
        'ルーム名': 'room_name',
        '時間帯': 'time_slot',
        '時間(h)': 'hours',
        '日': 'day_of_month',
        '種別': 'type' # 例: '通常' 'ボーナス'
    }
    
    # 既に標準化された名前があればそのまま、そうでなければマッピングを使用
    df.columns = [column_mapping.get(col, col) for col in df.columns]

    # 3. 'day_of_month' カラムから日付を生成し、'date' カラムを追加
    if 'day_of_month' in df.columns:
        year = month_str[:4]
        month = month_str[4:]
        
        # 日付が有効な数値であることを確認し、無効な行はスキップ
        df['day_of_month'] = pd.to_numeric(df['day_of_month'], errors='coerce')
        df.dropna(subset=['day_of_month'], inplace=True)
        df['day_of_month'] = df['day_of_month'].astype(int)
        
        # 1〜月末日までの範囲内の日であることを確認
        _, last_day = calendar.monthrange(int(year), int(month))
        df = df[(df['day_of_month'] >= 1) & (df['day_of_month'] <= last_day)].copy()
        
        df['date_str'] = df.apply(
            lambda row: f"{year}/{month}/{row['day_of_month']:02d}", 
            axis=1
        )
        # JSTのdatetimeオブジェクトに変換
        df['date'] = pd.to_datetime(df['date_str'], format='%Y/%m/%d').dt.tz_localize(JST)
        df.drop(columns=['date_str', 'day_of_month'], inplace=True, errors='ignore')
    
    # 4. 'hours'を数値に変換 (エラーがあればNaN、その後削除)
    if 'hours' in df.columns:
        df['hours'] = pd.to_numeric(df['hours'], errors='coerce')
        df.dropna(subset=['hours'], inplace=True)

    # 5. 不要なカラムを削除（元のカラム名全てが不明なため、暫定的に必須項目以外は削除）
    final_columns = ['date', 'room_name', 'time_slot', 'hours', 'type']
    df = df[[col for col in final_columns if col in df.columns]].copy()
    
    # 6. 'month'カラムを追加 (集計用に)
    df['month'] = month_str

    logging.info(f"Cleaned data for {month_str}: {len(df)} rows")
    return df


def ftp_upload(target_path, data_bytes):
    """指定されたデータをFTPでアップロードする"""
    try:
        logging.info(f"Connecting to FTP host: {FTP_CONFIG['host']}")
        with FTP(FTP_CONFIG["host"]) as ftp:
            ftp.login(user=FTP_CONFIG["user"], passwd=FTP_CONFIG["password"])
            ftp.encoding = 'utf-8'

            # StringIOではなくBytesIOを使用 (バイナリモード 'wb' のため)
            bio = io.BytesIO(data_bytes)
            
            # アップロード実行 (STOr file)
            ftp.storbinary(f'STOR {target_path}', bio)
            logging.info(f"Successfully uploaded to: {target_path}")
            return True

    except Exception as e:
        st.error(f"FTPアップロード中にエラーが発生しました。設定（ホスト名、ユーザー、パスワード、パス）を確認してください: {e}")
        logging.error(f"FTP Upload Error: {e}")
        return False


def run_data_update():
    """データ取得、整形、結合、FTPアップロードの一連の処理を実行する"""
    
    if not FTP_CONFIG:
        st.error("FTP設定が正しくロードされていません。")
        return

    # 1. 取得対象の年月リストを生成
    target_months = get_target_months(years=2) # 過去2年分
    
    st.info(f"⏳ 過去 {len(target_months)} ヶ月分のデータを取得します: {target_months[0]}〜{target_months[-1]}頃")
    
    all_data = []
    status_bar = st.progress(0)
    
    # 2. 各月のデータを取得・整形
    for i, month in enumerate(target_months):
        st.caption(f"Fetching data for {month}...")
        df = fetch_month_data(month, AUTH_COOKIE_STRING)
        if df is not None and not df.empty:
            all_data.append(df)
        
        status_bar.progress((i + 1) / len(target_months))

    status_bar.empty()
    
    if not all_data:
        st.error("😢 取得対象期間の有効なデータが見つかりませんでした。Cookieが有効か、期間内にデータが存在するか確認してください。")
        return
        
    # 3. 全データを結合
    final_df = pd.concat(all_data, ignore_index=True)
    
    # 4. 重複行の削除 (room_name, date, time_slot, typeが一致するものを最新のもののみ残す)
    # 重複判定のカラム
    # dateが最も重要なので、 date + room_name + time_slot + type が重複基準
    subset_cols = ['date', 'room_name', 'time_slot', 'type']
    final_df.sort_values(by='date', ascending=False, inplace=True) # 最新の日付を優先
    
    before_drop_count = len(final_df)
    final_df.drop_duplicates(subset=subset_cols, keep='first', inplace=True)
    after_drop_count = len(final_df)
    
    st.success(f"データ整形完了！合計 {after_drop_count} 件のレコードを統合しました。")
    if before_drop_count != after_drop_count:
        st.caption(f"({before_drop_count - after_drop_count} 件の古い重複データを除外しました。)")

    # 5. CSVデータに変換
    # タイムゾーン情報を除去して、YYYY-MM-DD HH:MM:SS形式の文字列として保存
    final_df['date'] = final_df['date'].dt.tz_convert(None).dt.strftime('%Y-%m-%d %H:%M:%S')

    csv_data = final_df.to_csv(index=False, encoding="utf-8-sig")
    csv_bytes = csv_data.encode("utf-8-sig")

    st.info(f"☁️ FTPサーバーへデータ ({TARGET_FILENAME}) をアップロード中...")
    
    # 6. FTPアップロード
    if ftp_upload(FTP_CONFIG["target_path"], csv_bytes):
        st.success("🎉 FTPアップロードが完了しました！")
        
        # 7. ダウンロードボタンを提供
        st.download_button(
            label="📥 統合されたCSVファイルをダウンロード",
            data=csv_bytes,
            file_name=f"showroom_time_charge_hist_{datetime.now(JST).strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    else:
        st.error("FTPアップロードに失敗しました。設定とサーバーのパスを確認してください。")


# --- Streamlit UI ---

st.set_page_config(page_title="SHOWROOMタイムチャージ履歴統合ツール", layout="centered")

st.title("💰 SHOWROOM タイムチャージ履歴統合ツール")
st.markdown("---")

st.markdown("""
このツールは、SHOWROOMオーガナイザーページから過去2年分のタイムチャージ履歴データを取得・統合し、
指定されたFTPサーバー上のCSVファイルを自動で更新します。
""")

if st.button("🚀 データ統合＆FTPアップロード実行"):
    run_data_update()

st.markdown("---")
st.caption("※ 実行には、有効なSHOWROOMオーガナイザーのCookieとFTP接続情報が`.streamlit/secrets.toml`に設定されている必要があります。")
st.caption(f"現在のFTPターゲットパス: `{FTP_CONFIG['target_path']}`")
