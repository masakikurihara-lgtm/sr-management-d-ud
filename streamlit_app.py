import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import calendar
from ftplib import FTP
import io
import pytz
import logging
from bs4 import BeautifulSoup 
import re 
from typing import List, Dict, Any

# ロギング設定 (デバッグ用)
logging.basicConfig(level=logging.INFO)

# 日本のタイムゾーン
JST = pytz.timezone('Asia/Tokyo')

# --- 定数設定 ---

# --- 売上データ設定 ---
SR_TIME_CHARGE_URL = "https://www.showroom-live.com/organizer/show_rank_time_charge_hist_invoice_format" 
SR_PREMIUM_LIVE_URL = "https://www.showroom-live.com/organizer/paid_live_hist_invoice_format" 
SR_ROOM_SALES_URL = "https://www.showroom-live.com/organizer/point_hist_with_mixed_rate" 

DATA_TYPES = {
    "time_charge": {
        "label": "タイムチャージ売上",
        "url": SR_TIME_CHARGE_URL,
        "filename": "show_rank_time_charge_hist_invoice_format.csv",
        "type": "standard" 
    },
    "premium_live": {
        "label": "プレミアムライブ売上",
        "url": SR_PREMIUM_LIVE_URL,
        "filename": "paid_live_hist_invoice_format.csv",
        "type": "standard"
    },
    "room_sales": { 
        "label": "ルーム売上",
        "url": SR_ROOM_SALES_URL,
        "filename": "point_hist_with_mixed_rate_csv_donwload_for_room.csv",
        "type": "room_sales"
    }
}

# --- KPIデータ設定 ---
SR_KPI_URL = "https://www.showroom-live.com/organizer/live_kpi"
KPI_MAX_PAGES = 5
# KPIデータの保存先ディレクトリ（売上データとは異なる絶対パスを定義）
KPI_FTP_BASE_PATH = "/mksoul-pro.com/showroom/csv/"


# --- 設定ロードと認証 (修正) ---
try:
    # 既存の共通Cookie（売上3点セット用）
    AUTH_COOKIE_STRING = st.secrets["showroom"]["auth_cookie_string"]
    
    # 🚨 修正: KPI専用Cookieの読み込みを試みる
    try:
        KPI_AUTH_COOKIE_STRING = st.secrets["showroom"]["kpi_auth_cookie_string"]
        st.info("KPI専用のCookieが設定されました。KPI処理ではこのCookieを使用します。")
    except KeyError:
        # KPI専用Cookieがsecretsにない場合は、共通Cookieをフォールバックとして使用
        KPI_AUTH_COOKIE_STRING = AUTH_COOKIE_STRING
        st.warning("KPI専用Cookie (`kpi_auth_cookie_string`) が見つかりません。共通CookieをKPI処理に使用します。")


    FTP_CONFIG = {
        "host": st.secrets["ftp"]["host"],
        "user": st.secrets["ftp"]["user"],
        "password": st.secrets["ftp"]["password"],
        "target_base_path": st.secrets["ftp"]["target_base_path"] 
    }
    
    # 売上データのベースパスがディレクトリパス（末尾が'/'）であることを保証
    if FTP_CONFIG["target_base_path"].endswith(".csv"):
        base_path = '/'.join(FTP_CONFIG["target_base_path"].split('/')[:-1]) + '/'
        FTP_CONFIG["target_base_path"] = base_path
    elif not FTP_CONFIG["target_base_path"].endswith('/'):
         FTP_CONFIG["target_base_path"] += '/'
    
except KeyError as e:
    AUTH_COOKIE_STRING = "DUMMY"
    KPI_AUTH_COOKIE_STRING = "DUMMY"
    FTP_CONFIG = None
    if str(e) == "'target_base_path'":
         st.error(f"🚨 FTP設定が不完全です。`target_path`ではなく`target_base_path`を`.streamlit/secrets.toml`で設定してください。")
    else:
        st.error(f"🚨 認証またはFTP設定がされていません。`.streamlit/secrets.toml`を確認してください。不足: {e}")
    st.stop()


# --- ユーティリティ関数 ---

def get_sales_months():
    """売上データ用: 2023年10月以降の月リストを 'YYYY年MM月分' 形式で生成し、UNIXタイムスタンプを計算する"""
    START_YEAR = 2023
    START_MONTH = 10 # 売上データは10月開始
    
    today = datetime.now(JST)
    months = []
    
    current_year = today.year
    current_month = today.month
    
    while True:
        if current_year < START_YEAR or (current_year == START_YEAR and current_month < START_MONTH):
            break

        month_str = f"{current_year}年{current_month:02d}月分"
        
        try:
            dt_naive = datetime(current_year, current_month, 1, 0, 0, 0)
            dt_obj_jst = JST.localize(dt_naive, is_dst=None)
            timestamp = int(dt_obj_jst.timestamp()) 
            
            months.append((month_str, timestamp)) 
        except Exception as e:
            logging.error(f"売上日付計算エラー ({month_str}): {e}")
            
        if current_month == 1:
            current_month = 12
            current_year -= 1
        else:
            current_month -= 1
            
    return months


def get_kpi_months():
    """KPIデータ用: 2023年9月以降の月リストを 'YYYY年MM月分' 形式で生成し、datetimeオブジェクトを計算する"""
    START_YEAR = 2023
    START_MONTH = 9 # KPIデータは9月開始
    
    today = datetime.now(JST)
    months = []
    
    current_year = today.year
    current_month = today.month
    
    while True:
        if current_year < START_YEAR or (current_year == START_YEAR and current_month < START_MONTH):
            break

        month_str = f"{current_year}年{current_month:02d}月分"
        
        try:
            dt_naive = datetime(current_year, current_month, 1, 0, 0, 0)
            
            months.append((month_str, dt_naive)) 
        except Exception as e:
            logging.error(f"KPI日付計算エラー ({month_str}): {e}")
            
        if current_month == 1:
            current_month = 12
            current_year -= 1
        else:
            current_month -= 1
            
    return months


def get_month_start_end_dates(month_dt: datetime) -> tuple[str, str, str]:
    """月の初日 ('YYYY-MM-01') と最終日 ('YYYY-MM-DD')、ファイル名プレフィックス ('YYYY-MM') を計算する"""
    from_date_str = month_dt.strftime('%Y-%m-01')
    
    if month_dt.month == 12:
        next_month = month_dt.replace(year=month_dt.year + 1, month=1, day=1)
    else:
        next_month = month_dt.replace(month=month_dt.month + 1, day=1)
        
    last_day = next_month - timedelta(days=1)
    to_date_str = last_day.strftime('%Y-%m-%d')
    
    file_prefix = month_dt.strftime('%Y-%m')
    
    return from_date_str, to_date_str, file_prefix


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

# --- 売上データ処理ロジック ---

def fetch_and_process_sales_data(timestamp, cookie_string, sr_url, data_type_key):
    """
    指定されたタイムスタンプに基づいてSHOWROOMから売上データを取得し、BeautifulSoupで整形する
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
        response.raise_for_status() 
        
        # 2. HTMLからのデータ抽出
        soup = BeautifulSoup(response.text, 'html5lib') 
        table = soup.find('table', class_='table-type-02') 
        
        if not table:
            if "ログイン" in response.text or "会員登録" in response.text:
                st.error("🚨 認証切れです。Cookieが古いか無効になっています。")
                return None
            st.warning("HTMLから売上データテーブル (`table-type-02`) を検出できませんでした。ページ構造が変更されたか、データがまだ生成されていません。")
            
        
        # 3. データをBeautifulSoupで抽出 (ライバー個別のデータ)
        table_data = []
        if table:
            rows = table.find_all('tr')
            
            for row in rows[1:]: 
                td_tags = row.find_all('td')
                
                if len(td_tags) >= 5:
                    amount_str = td_tags[3].text.strip().replace(',', '') 
                    account_id = td_tags[4].text.strip()
                    
                    if amount_str.isnumeric():
                         table_data.append({
                            '分配額': amount_str, 
                            'アカウントID': account_id
                        })
        
        # 4. DataFrameに変換し、整形 (ロジックの分岐)
        
        # 4-A. ルーム売上の特殊ロジック
        if data_type_key == "room_sales":
            
            total_amount_tag = soup.find('p', class_='fs-b4 bg-light-gray p-b3 mb-b2 link-light-green')
            total_amount_str = '0'
            if total_amount_tag:
                match = re.search(r'支払い金額（税抜）:\s*<span[^>]*>\s*([\d,]+)円', str(total_amount_tag))
                
                if match:
                    total_amount_str = match.group(1).replace(',', '') 
                else:
                    st.warning("⚠️ HTMLから「支払い金額（税抜）」の値を抽出できませんでした。分配額を「0」として処理を続行します。")
                    
            header_data = [{
                '分配額': total_amount_str,
                'アカウントID': 'MKsoul'
            }]
            
            header_df = pd.DataFrame(header_data)
            
            if table_data:
                driver_df = pd.DataFrame(table_data)
                df_cleaned = pd.concat([header_df, driver_df], ignore_index=True)
                st.success(f"テーブルデータ ({len(driver_df)}件) の抽出と合計値 ({total_amount_str}) の設定が完了しました。")
            else:
                df_cleaned = header_df
                st.warning(f"⚠️ ライバー個別のデータ行を抽出できませんでした。合計値 ({total_amount_str}) と MKsoul のみを含む1行データとして処理を続行します。")


        # 4-B. タイムチャージ/プレミアムライブの既存ロジック (0件時のダミーデータ生成)
        else: # time_charge or premium_live
            if not table_data:
                st.warning("⚠️ テーブルから有効なデータ行を抽出できませんでした。分配額=0、アカウントID=dummyを含む1行データとして処理を続行します。")
                
                df_cleaned = pd.DataFrame([{
                    '分配額': '0',       
                    'アカウントID': 'dummy' 
                }])
                
            else:
                st.success(f"テーブルデータ ({len(table_data)}件) の抽出が完了しました。")
                df_cleaned = pd.DataFrame(table_data)

        # 5. 特殊なCSV形式の作成（共通ロジック）
        
        now_jst = datetime.now(JST)
        update_time_str = now_jst.strftime('%Y/%m/%d %H:%M')
        
        final_df = pd.DataFrame({
            '分配額': df_cleaned['分配額'],
            'アカウントID': df_cleaned['アカウントID'],
            '更新日時': '' 
        })
        
        if not final_df.empty:
            final_df.loc[0, '更新日時'] = update_time_str
        
        csv_buffer = io.StringIO()
        final_df.to_csv(csv_buffer, index=False, header=False, encoding='utf-8')
        
        st.success("データの整形が完了しました。")
        st.code(csv_buffer.getvalue(), language='text') 
        
        return csv_buffer
        
    except requests.exceptions.HTTPError as e:
        st.error(f"HTTPエラーが発生しました: {e.response.status_code}. 認証Cookieが無効になっている可能性があります。")
        return None
    except Exception as e:
        st.error(f"予期せぬエラーが発生しました: {e}")
        logging.error("データ取得・整形エラー", exc_info=True)
        return None


# --- KPIデータ処理ロジック (新規) ---

def fetch_and_process_kpi_data(month_dt: datetime, cookie_string: str) -> pd.DataFrame or None:
    """
    指定された月（month_dt）に基づいてKPIデータを最大5ページ取得し、整形を行う
    """
    
    from_date_str, to_date_str, file_prefix = get_month_start_end_dates(month_dt)
    st.info(f"KPIデータ取得期間: {from_date_str} から {to_date_str} まで (最大 {KPI_MAX_PAGES} ページ)")
    # process_kpi_toolから渡された専用(またはフォールバック)のcookie_stringを使用
    session = create_authenticated_session(cookie_string) 
    if not session:
        return None
        
    # 前回試行したセッションウォームアップ処理は削除し、新Cookieでの認証に集中します。
    
    all_kpi_data: List[Dict[str, Any]] = []
    
    CSV_HEADERS = [
        "アカウントID", "ルームID", "配信日時", "配信時間(分)", "連続配信日数", "ルーム名", 
        "合計視聴数", "視聴会員数", "アクション会員数", "SPギフト使用会員率", "初ルーム来訪者数", 
        "初SR来訪者数", "短時間滞在者数", "ルームレベル", "フォロワー数", "フォロワー増減数", 
        "Post人数", "獲得支援point", "コメント数", "コメント人数", "初コメント人数", 
        "ギフト数", "ギフト人数", "初ギフト人数", "期限あり/期限なしSGのギフティング数", 
        "期限あり/期限なしSGのギフティング人数", "期限あり/期限なしSG総額", "2023年9月以前のおまけ分(無償SG RS外)"
    ]
    
    # ページネーションループ
    for page_num in range(1, KPI_MAX_PAGES + 1):
        try:
            url = (f"{SR_KPI_URL}?page={page_num}&room_id=&from_date={from_date_str}&to_date={to_date_str}")
            st.info(f"➡️ ページ {page_num} を取得中...")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
                'Referer': SR_KPI_URL
            }
            
            response = session.get(url, headers=headers, timeout=30)
            response.raise_for_status() 
            
            soup = BeautifulSoup(response.text, 'html5lib') 
            table = soup.find('table', class_='table-type-02') 
            
            if not table:
                if "ログイン" in response.text:
                    st.error("🚨 認証切れです。Cookieが古いか無効になっています。")
                    return None
                st.warning(f"ページ {page_num}: データテーブルを検出できませんでした。データが終了したか、ページ構造が変更されています。")
                break 

            rows = table.find_all('tr')
            
            page_data = []
            if len(rows) <= 1: 
                st.info(f"ページ {page_num}: 有効なデータ行がありませんでした。取得を終了します。")
                break 

            for row in rows[1:]: 
                td_tags = row.find_all('td')
                
                if len(td_tags) != 28:
                    continue 

                row_data: Dict[str, Any] = {}
                
                # 配信日時と配信時間(分)の特殊処理 (インデックス2)
                time_data = td_tags[2].text.strip()
                match_time = re.search(r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}).*?\((\d+)m(\d+)s\)', time_data)

                if match_time:
                    start_datetime_str = match_time.group(1).replace('-', '/')
                    minutes = int(match_time.group(2))
                    seconds = int(match_time.group(3))
                    duration_min = minutes + 1 if seconds >= 30 else minutes
                else:
                    start_datetime_str = ''
                    duration_min = 0

                row_data["配信日時"] = start_datetime_str
                row_data["配信時間(分)"] = str(duration_min)
                
                # その他の列の抽出
                for i, header in enumerate(CSV_HEADERS):
                    if i == 2 or header == "配信時間(分)":
                        continue
                        
                    content = td_tags[i].text.strip()
                    
                    if header in ["合計視聴数", "視聴会員数", "アクション会員数", "獲得支援point", "コメント数", "ギフト数", "フォロワー数", "ルームレベル", "初ルーム来訪者数", "初SR来訪者数", "短時間滞在者数", "フォロワー増減数", "Post人数", "コメント人数", "初コメント人数", "ギフト人数", "初ギフト人数", "期限あり/期限なしSGのギフティング数", "期限あり/期限なしSGのギフティング人数", "期限あり/期限なしSG総額"]:
                        content = content.replace(',', '')
                    elif header == "SPギフト使用会員率":
                        content = content.replace('%', '')
                    elif header == "ルーム名":
                        div_tag = td_tags[i].find('div')
                        if div_tag:
                            content = div_tag.text.strip()

                    if i in [0, 1]:
                        a_tag = td_tags[i].find('a')
                        if a_tag:
                            content = a_tag.text.strip()
                        
                    row_data[header] = content
                    
                page_data.append(row_data)

            if page_data:
                st.success(f"ページ {page_num}: {len(page_data)}件のデータを抽出しました。")
                all_kpi_data.extend(page_data)
            
            if len(page_data) < 1000:
                 st.info(f"ページ {page_num} の取得件数が1000件未満だったため、データ取得を終了します。")
                 break

        except requests.exceptions.HTTPError as e:
            st.error(f"ページ {page_num} でHTTPエラーが発生しました: {e.response.status_code}. 認証Cookieが無効になっている可能性があります。")
            return None
        except Exception as e:
            st.error(f"ページ {page_num} で予期せぬエラーが発生しました: {e}")
            logging.error(f"KPIデータ取得エラー (ページ {page_num})", exc_info=True)
            return None
    
    if not all_kpi_data:
        st.warning("⚠️ 期間内のKPIデータが全く抽出されませんでした。")
        return pd.DataFrame(columns=CSV_HEADERS)
    
    df = pd.DataFrame(all_kpi_data, columns=CSV_HEADERS)
    
    # 重複除外 (重複除外キーはアカウントID, ルームID, 配信日時, 配信時間(分))
    dedup_keys = ["アカウントID", "ルームID", "配信日時", "配信時間(分)"]
    original_count = len(df)
    df_cleaned = df.drop_duplicates(subset=dedup_keys, keep='first')
    dedup_count = len(df_cleaned)

    if original_count > dedup_count:
        st.success(f"データ取得・整形が完了しました。重複データを {original_count - dedup_count} 件除外しました。最終件数: {dedup_count} 件。")
    else:
        st.success(f"データ取得・整形が完了しました。最終件数: {dedup_count} 件。")

    return df_cleaned


# --- FTPアップロード関数 ---
def upload_file_ftp(csv_buffer, ftp_config, full_target_path):
    # ... (変更なし) ...
    """
    FTPサーバーに整形済みCSVファイルをアップロードする 
    """
    st.info(f"FTPサーバー ({ftp_config['host']}) に接続し、ファイルをアップロードします... (パス: {full_target_path})")
    
    try:
        csv_buffer.seek(0)
        # FTP接続
        with FTP(ftp_config['host'], ftp_config['user'], ftp_config['password']) as ftp:
            csv_bytes = csv_buffer.getvalue().encode('utf-8')
            
            ftp.storbinary(f'STOR {full_target_path}', io.BytesIO(csv_bytes))
            
            st.success(f"✅ ファイルのアップロードが完了しました！")
            st.markdown(f"**アップロード先:** `{ftp_config['host']}:{full_target_path}`")
            
    except Exception as e:
        st.error(f"FTPアップロード中にエラーが発生しました。設定（ホスト名、ユーザー、パスワード、パス）を確認してください: {e}")
        logging.error("FTPエラー", exc_info=True)
        return False
        
    return True


# --- ラッパー関数 ---

def process_sales_tool(data_type_key, selected_timestamp, auth_cookie_string, ftp_config):
    # ... (変更なし) ...
    """
    売上データタイプ（タイムチャージ、プレミアムライブ、またはルーム売上）の処理を実行する
    """
    data_info = DATA_TYPES[data_type_key]
    data_label = data_info["label"]
    sr_url = data_info["url"]
    filename = data_info["filename"]
    
    full_target_path = ftp_config["target_base_path"] + filename
    
    st.subheader(f"🔄 **{data_label}** の処理を開始します")
    
    csv_buffer = fetch_and_process_sales_data(selected_timestamp, auth_cookie_string, sr_url, data_type_key)
    
    if csv_buffer:
        if ftp_config:
            upload_file_ftp(csv_buffer, ftp_config, full_target_path)
        else:
            st.error("FTP設定が読み込まれていないため、アップロードはスキップされました。")
    else:
        st.error(f"{data_label}のデータ取得・整形に失敗したため、アップロードはスキップされました。")
        
    st.markdown("---")

def process_kpi_tool(selected_month_dt_list: List[datetime], auth_cookie_string: str, ftp_config: Dict[str, str]):
    # ... (引数のauth_cookie_stringにはKPI_AUTH_COOKIE_STRINGが渡される) ...
    """
    KPIデータ取得・整形・アップロードの処理を複数月に対して実行する
    """
    
    if not selected_month_dt_list:
        st.warning("⚠️ 処理対象の月が選択されていません。")
        return
        
    selected_month_dt_list.sort() 
    
    st.subheader(f"📊 **配信KPIデータ** の処理を開始します ({len(selected_month_dt_list)}ヶ月分)")
    
    for month_dt in selected_month_dt_list:
        month_label = month_dt.strftime('%Y年%m月分')
        st.info(f"--- {month_label} の処理 ---")
        
        from_date_str, to_date_str, file_prefix = get_month_start_end_dates(month_dt)
        target_filename = f"{file_prefix}_all_all.csv"
        
        full_target_path = KPI_FTP_BASE_PATH + target_filename
        
        # 1. データ取得と整形（DataFrameを返す）
        df_cleaned = fetch_and_process_kpi_data(month_dt, auth_cookie_string) # 渡されたCookieを使用
        
        if df_cleaned is not None:
            
            # 2. CSVデータとしてメモリに書き出す
            csv_buffer = io.StringIO()
            df_cleaned.to_csv(csv_buffer, index=False, header=True, encoding='utf-8')
            
            st.success(f"【{month_label}】のデータ整形が完了しました。件数: {len(df_cleaned)}件。")
            st.code(csv_buffer.getvalue()[:2000] + "\n...", language='csv') 
            
            # 3. FTPアップロード
            if ftp_config:
                upload_file_ftp(csv_buffer, ftp_config, full_target_path)
            else:
                st.error("FTP設定が読み込まれていないため、アップロードはスキップされました。")
        else:
            st.error(f"【{month_label}】のデータ取得・整形に失敗したため、アップロードはスキップされました。")
            
        st.markdown("---")


# --- Streamlit UI (修正) ---

def main():
    st.set_page_config(page_title="SHOWROOMデータ アップロードツール", layout="wide")
    st.title("SHOWROOMデータ 自動アップロードツール (売上3種 & 配信KPI)")
    st.markdown("---")

    
    # 1. 売上データ用の月選択 (2023年10月以降、シングルセレクト)
    sales_month_options = get_sales_months()
    sales_month_labels = [label for label, _ in sales_month_options]
    
    st.header("1. 売上データ（3種）処理対象月選択")
    
    selected_sales_label = st.selectbox(
        "売上データ（2023年10月以降）の処理対象月を選択してください:",
        options=sales_month_labels,
        index=0 
    )
    
    selected_sales_timestamp = next((ts for label, ts in sales_month_options if label == selected_sales_label), None)

    if selected_sales_timestamp is None:
        st.warning("有効な売上処理月が選択されていません。")
        return
        
    st.info(f"選択された売上処理月: **{selected_sales_label}**")
    
    # --- 売上データ一括処理ボタン ---
    if st.button("🚀 売上データ3種（タイムチャージ/プレミアムライブ/ルーム売上）を取得・FTPアップロードを実行", type="primary"):
        with st.spinner(f"売上データ処理中: {selected_sales_label}のデータを取得しています..."):
            
            # 共通Cookie (AUTH_COOKIE_STRING) を使用
            process_sales_tool("time_charge", selected_sales_timestamp, AUTH_COOKIE_STRING, FTP_CONFIG)
            process_sales_tool("premium_live", selected_sales_timestamp, AUTH_COOKIE_STRING, FTP_CONFIG)
            process_sales_tool("room_sales", selected_sales_timestamp, AUTH_COOKIE_STRING, FTP_CONFIG)

        st.balloons()
        st.success("🎉 **売上データ3種の全ての処理が完了しました！**")
        
    st.markdown("---")

    # 2. KPIデータ用の月選択 (2023年9月以降、マルチセレクト)
    kpi_month_options = get_kpi_months()
    kpi_month_labels = [label for label, _ in kpi_month_options]
    
    st.header("2. 配信KPIデータ処理対象月選択")
    
    default_selection = kpi_month_labels[0] if kpi_month_labels else None
    
    selected_kpi_labels = st.multiselect(
        "配信KPIデータ（2023年9月以降）の処理対象月を複数選択してください:",
        options=kpi_month_labels,
        default=[default_selection] if default_selection else []
    )
    
    selected_kpi_dt_list = [dt for label, dt in kpi_month_options if label in selected_kpi_labels]

    if selected_kpi_labels:
        st.info(f"選択されたKPI処理月: **{', '.join(selected_kpi_labels)}**")
    
    # --- KPIデータ処理ボタン ---
    if st.button("📊 配信KPIデータ を取得・FTPアップロードを実行", type="secondary"):
        with st.spinner(f"KPIデータ処理中: 選択された月 ({len(selected_kpi_dt_list)}ヶ月) のKPIデータを取得しています..."):
            
            # 🚨 修正: KPI専用Cookie (KPI_AUTH_COOKIE_STRING) を使用
            process_kpi_tool(selected_kpi_dt_list, KPI_AUTH_COOKIE_STRING, FTP_CONFIG)

        st.balloons()
        st.success("🎉 **配信KPIデータの処理が完了しました！**")


if __name__ == "__main__":
    main()