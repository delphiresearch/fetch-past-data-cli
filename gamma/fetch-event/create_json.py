import json
import os

def create_json_file(data, filename):
    """
    JSONデータをファイルとして保存する関数
    
    Args:
        data: JSON形式に変換可能なPythonオブジェクト
        filename: 作成するJSONファイルの名前（.json拡張子は自動で追加）
    """
    # データが空の配列かチェック
    if isinstance(data, list) and len(data) == 0:
        print("エラー: データが空の配列です。JSONファイルは作成されませんでした。")
        return
    
    # ファイル名に.json拡張子がない場合は追加
    if not filename.endswith('.json'):
        filename += '.json'
    
    # 現在のスクリプトと同じディレクトリにファイルを作成
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, "output", filename)
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"JSONファイルが正常に作成されました: {file_path}")
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")

