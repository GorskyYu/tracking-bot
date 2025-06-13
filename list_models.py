# list_models.py（更新版，適用於 openai>=1.0.0）

import openai
import os

# 1) 先讀取環境變數裡的 API Key
openai.api_key = os.getenv("OPENAI_API_KEY")

def list_available_models():
    try:
        # 2) 呼叫新版 API：openai.models.list()
        resp = openai.models.list()

        # resp.data 是一個 List[Model]，其中每個 Model 物件有屬性 id
        model_ids = [model.id for model in resp.data]

        print("Available models:\n")
        for mid in model_ids:
            print(mid)

    except Exception as e:
        print("Error listing models:", e)

if __name__ == "__main__":
    list_available_models()
