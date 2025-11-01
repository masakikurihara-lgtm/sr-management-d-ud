import streamlit as st
import requests
import pandas as pd
from datetime import datetime
import calendar
from ftplib import FTP
import io
import pytz
import logging

# ロギング設定 (Streamlit Cloudでのデバッグ用)
logging.basicConfig(level=logging.INFO)

# --- 定数設定 ---
# 取得元の基本URL
SR_BASE_URL = "https://www.showroom-live.com/organizer/show_rank_time_charge_hist_invoice_format" 
# アップロード先ファイル名
TARGET_FILENAME = "show_rank_time_charge_hist_invoice_format.csv"
# 日本のタイムゾーン
JST = pytz.timezone('Asia/Tokyo')

# --- ユーティリティ関数 ---

def get_target_months(years=2):
    """過去N年間の月リストを 'YYYY年MM月分' 形式で生成し、正確なUNIXタイムスタンプを計算する"""
    today = datetime.now(JST)
    months = []
    
    # 選択肢の表示を当月含む過去2年分程度に限定
    for y in range(today.year - years + 1, today.year + 1):
        for m in range(1, 13):
            # 今後の月は除外
            if y == today.year and m > today.month:
                continue 
            
            month_str = f"{y}年{m:02d}月分"
            
            try:
                # 文字列から確実に年と月を取得し直す
                calc_y = int(month_str[:4])
                calc_m = int(month_str[5:7])

                # --- タイムスタンプ計算の修正ロジック ---
                # 1. タイムゾーン情報のないdatetimeオブジェクトを生成
                dt_naive = datetime(calc_y, calc_m, 1, 0, 0, 0)
                
                # 2. JSTでローカライズ
                dt_obj_jst = JST.localize(dt_naive, is_dst=None)
                
                # 3. UNIXタイムスタンプ（UTC基準）に変換
                timestamp = int(dt_obj_jst.timestamp()) 
                
                # --- デバッグ検証用：2025年10月分の確認 ---
                if calc_y == 2025 and calc_m == 10:
                    # 期待値: 1759244400
                    expected_ts = 1759244400
                    logging.info(f"DEBUG: 2025/10/01 00:00:00 JST (計算結果): {timestamp}. 期待値: {expected_ts}")
                    if timestamp != expected_ts:
                         logging.error(f"重大エラー: 2025年10月の計算結果が期待値と一致しません。計算結果: {timestamp}")
                # --- デバッグ検証用：2025年9月分の確認 ---
                if calc_y == 2025 and calc_m == 9:
                    # 期待値: 1756652400
                    expected_ts = 1756652400
                    logging.info(f"DEBUG: 2025/09/01 00:00:00 JST (計算結果): {timestamp}. 期待値: {expected_ts}")
                    if timestamp != expected_ts:
                         logging.error(f"重大エラー: 2025年9月の計算結果が期待値と一致しません。計算結果: {timestamp}")
                # ==========================================

                months.append((month_str, timestamp))
            except Exception as e:
                logging.error(f"日付計算エラー ({month_str}): {e}")
                continue
                
    # 最新の月が上に来るように逆順にする
    return months[::-1]

def fetch_and_process_data(timestamp, cookie_string):
    """
    指定されたタイムスタンプに基づいてSHOWROOMからデータを取得し、整形する
    """
    st.info(f"データ取得中... タイムスタンプ: {timestamp}")
    
    try:
        # 1. データ取得
        # SR_BASE_URLを使用
        url = f"{SR_BASE_URL}?from={timestamp}" 
        headers = {
            # 認証に必要なCookieを設定
            "Cookie": cookie_string,
            # ブラウザとして振る舞うためのUser-Agentを設定
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
        }
        
        session = requests.Session()
        response = session.get(url, headers=headers, timeout=30)
        response.raise_for_status() # HTTPエラーが発生した場合に例外を発生させる
        
        # 2. HTMLからのデータ抽出 (pandas.read_htmlを使用)
        # lxmlがrequirements.txtに追加されているため、正常に動作するはず
        tables = pd.read_html(response.text)
        
        if not tables:
            st.error("HTMLからテーブルデータ（売上情報）を検出できませんでした。Cookieが正しく設定されているか、またはデータがページ上に表示されているか確認してください。")
            return None
        
        # 最初のテーブルが売上データ（ルームID, ルームURL, ルーム名, 分配額, アカウントIDを含む）と仮定
        raw_df = tables[0]
        st.success(f"テーブルデータを取得しました。行数: {len(raw_df)}")
        
        # 取得したテーブルのヘッダーを表示して確認
        st.markdown("##### 取得データ（ヘッダーと最初の5行）")
        st.dataframe(raw_df.head())
        
        # 3. データ整形
        
        # --- データ整形ロジック（要件の特殊CSVフォーマットに合わせる） ---
        
        # 提供されたHTMLスニペットに基づき、必要な列のインデックスを設定
        # 0: ルームID, 1: ルームURL, 2: ルーム名, 3: 分配額, 4: アカウントID
        # CSVの1列目: 分配額 (インデックス 3)
        # CSVの2列目: アカウントID (インデックス 4)
        
        AMOUNT_COL = 3
        ACCOUNT_ID_COL = 4 
        
        if ACCOUNT_ID_COL >= len(raw_df.columns) or AMOUNT_COL >= len(raw_df.columns):
            st.warning("DataFrameの列数が不足しています。指定されたインデックス（分配額: 3, アカウントID: 4）がデータと一致しない可能性があります。")
            return None
        
        # 分配額とアカウントIDの列を抽出
        # CSV出力の順番に合わせて [分配額の列, アカウントIDの列] の順で抽出
        df_extracted = raw_df.iloc[:, [AMOUNT_COL, ACCOUNT_ID_COL]].copy()
        
        # DataFrameのインデックスとヘッダーをリセット (整形のために一時的に設定)
        df_extracted.columns = ['分配額', 'アカウントID']
        
        # NaNや合計行などを削除 (分配額が数値でない行を削除するなど)
        # 抽出した分配額の列を文字列に変換し、数値のみを含む行にフィルタリング
        # また、分配額からカンマ(,)を除去してからisnumeric()を適用する処理を追加
        df_extracted['分配額_cleaned'] = df_extracted['分配額'].astype(str).str.replace(',', '', regex=False)
        df_cleaned = df_extracted[df_extracted['分配額_cleaned'].str.isnumeric()].copy()
        df_cleaned['分配額'] = df_cleaned['分配額_cleaned'] # クリーンアップした分配額で上書き
        
        # 4. 特殊なヘッダー行の作成
        
        # 現在時刻を日本時間で取得
        now_jst = datetime.now(JST)
        update_time_str = now_jst.strftime('%Y/%m/%d %H:%M')
        
        # ヘッダー行 (1行目のみ3列目に更新日時)
        # 1列目: '', 2列目: '', 3列目: '更新日時'
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
        # ヘッダーなし、インデックスなし、コンマ区切りで書き出す
        # to_csvのencodingには、要件にある「CSV UTF-8」を指定
        final_df.to_csv(csv_buffer, index=False, header=False, encoding='utf-8')
        
        st.success("データの整形が完了しました。")
        st.code(csv_buffer.getvalue().split('\n')[:5], language='text') # 整形後のCSVプレビュー
        
        return csv_buffer
        
    except requests.HTTPError as e:
        st.error(f"HTTPエラーが発生しました: {e.response.status_code}. 認証Cookieが無効になっている可能性があります。")
        return None
    except ValueError as e:
        # lxmlエラーを捕捉し、より具体的なメッセージを表示する
        if "lxml" in str(e):
             # requirements.txtが導入されたため、このエラーは発生しないはずですが、念のため捕捉
             st.error("【重要】データ取得に失敗しました。HTML解析に必要なライブラリ `lxml` が見つかりません。")
             st.error("Streamlit環境の設定ファイル（`requirements.txt`など）に `lxml` の追加が必要です。")
             st.stop() # 処理を停止
        else:
             st.error("データの抽出に失敗しました。取得したHTML内にテーブルデータが見つからないか、データが期待する形式と異なります。")
             st.error("`pandas.read_html`が売上テーブルを特定できませんでした。CookieやHTML構造を再確認してください。")
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
            # ftplibのstorbinaryはバイナリモードで転送するため、StringIOをBytesIOに変換する
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

    # 1. secretsからの設定ロード
    try:
        sr_config = st.secrets["showroom"]
        ftp_config = st.secrets["ftp"]
    except KeyError:
        st.error("Secretsの設定が不足しています。`.streamlit/secrets.toml`を確認し、[showroom]と[ftp]セクションが存在することを確認してください。")
        return

    # 2. 月選択プルダウンの作成
    
    # 選択肢のリストを取得 (['YYYY年MM月分', timestamp])
    month_options = get_target_months()
    
    # プルダウンに表示する文字列のみを抽出
    month_labels = [label for label, _ in month_options]
    
    st.header("1. 対象月選択")
    
    selected_label = st.selectbox(
        "処理対象の配信月を選択してください:",
        options=month_labels,
        index=0 # デフォルトで最新の月を選択
    )
    
    # 選択されたラベルから対応するタイムスタンプを検索
    selected_timestamp = next((ts for label, ts in month_options if label == selected_label), None)

    if selected_timestamp is None:
        st.warning("有効な月が選択されていません。")
        return
        
    # 確認のための出力。ここで正しいタイムスタンプが表示されるはずです。
    st.info(f"選択された月: **{selected_label}** (UNIXタイムスタンプ: {selected_timestamp})")
    
    st.header("2. データ取得とアップロードの実行")
    
    # 3. 実行ボタン
    if st.button("🚀 データ取得・整形・FTPアップロードを実行", type="primary"):
        with st.spinner(f"処理中: {selected_label}のデータを取得しています..."):
            
            # 1. データ取得と整形
            csv_buffer = fetch_and_process_data(selected_timestamp, sr_config['auth_cookie_string'])
            
            if csv_buffer:
                # 2. FTPアップロード
                upload_file_ftp(csv_buffer, ftp_config)
            else:
                st.error("データ取得・整形に失敗したため、アップロードはスキップされました。")

if __name__ == "__main__":
    main()
