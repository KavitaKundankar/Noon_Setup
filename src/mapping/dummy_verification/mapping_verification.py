import google.generativeai as genai
import json, re

def build_llm_mapping_prompt(unmapped_keys, standard_keys):
    prompt = f"""
You are an expert in maritime noon-report data mapping.

Your task:
1. Carefully analyze the **unmapped keys**.
2. Try to match each unmapped key with the most appropriate **standard key**.
3. If no suitable match exists, set the value to null.
4. return only flat JSON


Input:
- **Unmapped Keys**: {unmapped_keys}
- **Standard Keys**: {standard_keys}

Rules:
- Only use keys from the provided standard keys list.
- Do NOT invent new standard keys.
- Always return valid JSON, no explanations outside the JSON.
- Use best-effort semantic understanding (example: 'me_cyl_oil_last' → 'Total_ME_CYL_Oil_Consumed_Low_Sulphur').
"""
    
    model = genai.GenerativeModel("models/gemini-2.5-flash-lite")
    response = model.generate_content(prompt)
    llm_mapped = re.sub(r"^```json\s*|\s*```$", "", response.text.strip())
    
        
    return llm_mapped