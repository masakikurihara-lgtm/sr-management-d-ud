import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from io import StringIO
from ftplib import FTP
import time

# --- 設定（Streamlit Secretsから読み込む） ---
try:
    # Streamlit Secretsから認証情報とFTP情報を取得
    SR_LOGIN_ID = st.secrets["showroom"]["login_id"]
    SR_PASSWORD = st.secrets["showroom"]["password"]
    SR_COOKIE = st.secrets["showroom"]["auth_cookie_string"]
    
    FTP_HOST = st.secrets["ftp"]["host"]
    FTP_USER = st.secrets["ftp"]["user"]
    FTP_PASSWORD = st.secrets["ftp"]["password"]
except KeyError as e:
    st.error(f"Streamlit Secretsの設定が不足しています。キー '{e}' が見つかりません。secrets.tomlを確認してください。")
    st.stop()


# --- 定数 ---
# タイムチャージ売上データの取得元URL（fromパラメータは後で置換）
BASE_URL = "https://www.showroom-live.com/organizer/show_rank_time_charge_hist_invoice_format?from={}"
# アップロード先のファイルパス（FTP）
FTP_UPLOAD_PATH = "/showroom/sales-app_v2/db/show_rank_time_charge_hist_invoice_format.csv"

# タイムゾーンを日本時間 (JST) に設定
JST = timezone(timedelta(hours=+9), 'JST')


# --- 関数定義 ---

def get_month_options():
    """過去3ヶ月分の「YYYY年M月分」のオプションとUnixタイムスタンプを生成する"""
    options = {}
    today = datetime.now(JST)
    
    # 翌月、今月、先月、先々月の4ヶ月分を考慮
    # 翌月分 (例: 11月1日に10月分を見る)
    current_month_start = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    for i in range(4): # 4ヶ月分を生成 (例: 10月分, 9月分, 8月分, 7月分)
        target_month_start = (current_month_start - timedelta(days=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        # 配信月 (例: 2025年10月分)
        display_month = f"{target_month_start.year}年{target_month_start.month}月分"
        
        # URLのfromパラメータは「その月の1日の00:00:00」のUnixタイムスタンプ
        unix_timestamp = int(target_month_start.timestamp())
        
        options[display_month] = unix_timestamp
        
        # 次のループのために1ヶ月戻す
        current_month_start = target_month_start

    return options

def get_data_from_showroom(url, cookie_string):
    """認証Cookieを使ってSHOWROOMのURLからデータを取得する"""
    headers = {
        'Cookie': cookie_string,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    st.info(f"データ取得URL: {url}")
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status() # HTTPエラーが発生した場合に例外を発生させる
        return response
    except requests.exceptions.RequestException as e:
        st.error(f"データ取得エラー: {e}")
        return None

def process_and_format_data(response_content):
    """取得したデータ（HTML/TSV）をパースし、指定されたCSV形式に整形する"""
    current_time = datetime.now(JST).strftime("%Y/%m/%d %H:%M")
    
    # TSV/CSVデータ部分の抽出（HTMLレスポンスの場合はスクレイピングが必要）
    # 仮にダウンロードボタンの裏側でTSV/CSVが返るURL（上記BASE_URL）を叩いていると仮定し、
    # レスポンスがそのままTSV/CSV形式のデータであるとして処理を続行します。
    
    try:
        # TSVデータとして読み込む
        # SHOWROOMのデータはタブ区切りが多いと想定されるため、sep='\t'を試行
        df = pd.read_csv(StringIO(response_content.text), sep='\t', header=None, encoding='utf-8')
    except Exception as e:
        st.warning(f"TSV形式でのパースに失敗しました。エラー: {e}")
        # CSV（カンマ区切り）として再試行
        try:
            df = pd.read_csv(StringIO(response_content.text), sep=',', header=None, encoding='utf-8')
        except Exception as e:
            st.error(f"CSV形式でのパースにも失敗しました。エラー: {e}")
            st.error("レスポンスデータが期待されるTSV/CSV形式ではありません。ダウンロード機能が働いていない可能性があります。")
            st.code(response_content.text[:500] + '...') # データの冒頭を表示してデバッグを促す
            return None

    st.success(f"データ ({len(df)}行) のパースに成功しました。")
    
    # 既存のCSVファイル (show_rank_time_charge_hist_invoice_format.csv) の構造に合わせて整形
    # 想定: 1列目: 分配額, 2列目: アカウントID
    
    # 必要な列が少なくとも2列あるか確認
    if df.shape[1] < 2:
        st.error(f"取得データには少なくとも2列（分配額、アカウントID）が必要です。現在の列数: {df.shape[1]}")
        return None

    # ヘッダーなし、1列目(分配額), 2列目(アカウントID)を抽出・整形
    processed_df = df.iloc[:, [0, 1]].copy()
    
    # 分配額(1列目)を数値型に変換し、カンマを除去
    processed_df[0] = processed_df[0].astype(str).str.replace(r'[^\d]', '', regex=True).replace('', 0).astype(int)
    
    # 最終的なCSVデータ（ヘッダーなし、1行目に更新日時を追加）
    csv_data = StringIO()
    
    # 1行目に更新日時を追加（「分配額,アカウントID,2025/11/1 2:55」の形式）
    # 既存のCSVの1行目を確認すると、1列目に分配額、2列目にアカウントID、3列目に更新日時が記載されている。
    # このコードでは、最初の行を抽出して更新日時をセットする。
    first_row_data = processed_df.iloc[0].tolist()
    first_row_data.append(current_time)
    
    # 1行目を出力
    csv_data.write(",".join(map(str, first_row_data)) + "\n")
    
    # 2行目以降（アカウントIDと売上データ）を出力
    # 2行目以降の行については、3列目は空（アカウントIDと売上データのみ）
    # ただし、既存のCSVファイルを見ると、2行目以降も3列目が空の状態で存在している
    # → 2行目以降は、分配額とアカウントIDのみをカンマ区切りで出力する
    processed_df.iloc[1:].to_csv(csv_data, header=False, index=False, columns=[0, 1], encoding='utf-8', line_terminator='\n')
    
    return csv_data.getvalue()


def upload_to_ftp(data_string, ftp_host, ftp_user, ftp_password, remote_path):
    """FTPでファイルをアップロードする"""
    try:
        with FTP(ftp_host) as ftp:
            st.info(f"FTPサーバーに接続中: {ftp_host}")
            ftp.login(user=ftp_user, passwd=ftp_password)
            st.success("FTPログイン成功")
            
            # リモートディレクトリに移動
            dir_name = os.path.dirname(remote_path)
            file_name = os.path.basename(remote_path)
            
            if dir_name:
                try:
                    ftp.cwd(dir_name)
                    st.info(f"ディレクトリ移動: {dir_name}")
                except Exception as e:
                    st.error(f"FTPディレクトリ移動エラー: {dir_name} が存在しません。エラー: {e}")
                    return False
            
            # データ（文字列）をファイルとしてアップロード
            data_io = StringIO(data_string)
            ftp.storlines(f"STOR {file_name}", data_io)
            
            st.success(f"ファイルアップロード成功: {remote_path}")
            return True

    except Exception as e:
        st.error(f"FTPアップロードエラー: {e}")
        return False

# --- Streamlit UI ---

import os
st.set_page_config(page_title="売上データ更新ツール", layout="centered")
st.title("タイムチャージ売上データ更新ツール")
st.markdown("SHOWROOMオーガナイザー管理画面からデータを取得し、整形してFTPサーバー上のCSVファイルを更新します。")

# 月度選択プルダウン
month_options = get_month_options()
sorted_months = sorted(month_options.keys(), reverse=True)
selected_month = st.selectbox(
    "更新対象の月度を選択してください:",
    sorted_months
)

# 選択された月のUnixタイムスタンプ
unix_timestamp = month_options[selected_month]
target_url = BASE_URL.format(unix_timestamp)

if st.button(f"{selected_month} のデータを取得・更新"):
    st.info(f"処理を開始します: {selected_month} (Unix Time: {unix_timestamp})")
    
    with st.spinner("STEP 1/3: SHOWROOMからデータを取得中..."):
        response = get_data_from_showroom(target_url, SR_COOKIE)
        
    if response and response.status_code == 200:
        st.success("STEP 1/3: データ取得成功。")
        
        with st.spinner("STEP 2/3: データを整形中..."):
            # レスポンスエンコーディングの確認とデコード
            try:
                 # レスポンスがTSV/CSVであるため、textプロパティを使用。
                 # Content-Typeが設定されていればencodingが推測されるが、
                 # 強制的にUTF-8（またはSHIFT-JIS）を試すことが推奨される場合もある。
                 # ここではrequestsの自動推測（response.text）を使用。
                 formatted_csv_content = process_and_format_data(response)
            except Exception as e:
                 st.error(f"データ加工中に予期せぬエラーが発生しました: {e}")
                 formatted_csv_content = None

        if formatted_csv_content:
            st.success("STEP 2/3: データ整形完了。")
            
            # --- 結果のプレビュー ---
            st.subheader("整形されたCSVデータ (プレビュー)")
            st.text(formatted_csv_content[:500] + '...' if len(formatted_csv_content) > 500 else formatted_csv_content)
            
            # --- FTPアップロード ---
            with st.spinner("STEP 3/3: FTPサーバーにアップロード中..."):
                upload_success = upload_to_ftp(
                    formatted_csv_content,
                    FTP_HOST,
                    FTP_USER,
                    FTP_PASSWORD,
                    FTP_UPLOAD_PATH
                )

            if upload_success:
                st.balloons()
                st.markdown(f"## :white_check_mark: 処理完了！")
                st.markdown(f"**{selected_month}** のタイムチャージ売上データが更新されました。")
                st.markdown(f"**アップロード先:** `{FTP_UPLOAD_PATH}`")
                st.markdown(f"**更新日時:** `{datetime.now(JST).strftime('%Y/%m/%d %H:%M')}`")
            else:
                st.error("STEP 3/3: FTPアップロードに失敗しました。詳細については、上記のエラーメッセージを確認してください。")
        else:
             st.error("STEP 2/3: データ整形が完了しなかったため、アップロードを中止しました。")
    else:
        st.error("STEP 1/3: データ取得に失敗しました。Cookieが有効か、URLが正しいか確認してください。")
