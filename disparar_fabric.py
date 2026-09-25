# -*- coding: utf-8 -*-
"""
Disparador do Pipeline no Microsoft Fabric via API ou Webhook
"""
import os
import sys
import requests

tenant_id = os.environ.get("AZURE_TENANT_ID")
client_id = os.environ.get("AZURE_CLIENT_ID")
client_secret = os.environ.get("AZURE_CLIENT_SECRET")
webhook_url = os.environ.get("FABRIC_WEBHOOK_URL")
workspace_id = os.environ.get("WORKSPACE_ID", "0c2a1a5e-4519-4e4e-b3e6-14288c11291d")
pipeline_id = os.environ.get("PIPELINE_ID", "fd4247e7-462e-4a39-8df4-4d4854da6311")

print(f"[*] Alvo: Workspace {workspace_id} | Pipeline {pipeline_id}")

# 1. Disparo via Webhook do Power Automate
if webhook_url and webhook_url.strip():
    print("[*] Disparando via Webhook seguro do Power Automate...")
    try:
        r = requests.post(webhook_url.strip(), json={"origem": "GitHub Actions", "pipeline_id": pipeline_id}, timeout=30)
        print(f"Status Webhook: {r.status_code}")
        if r.status_code in [200, 202]:
            print("✅ Pipeline disparado com sucesso no Fabric via Webhook!")
            sys.exit(0)
        else:
            print(f"❌ Erro ao disparar webhook: {r.text}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Exceção ao chamar webhook: {e}")
        sys.exit(1)

# 2. Disparo via Service Principal do Azure Entra ID
if client_id and client_secret and tenant_id:
    print("[*] Autenticando no Azure Entra ID (Service Principal)...")
    token_url = f"https://login.microsoftonline.com/{tenant_id.strip()}/oauth2/v2.0/token"
    token_data = {
        "client_id": client_id.strip(),
        "client_secret": client_secret.strip(),
        "grant_type": "client_credentials",
        "scope": "https://api.fabric.microsoft.com/.default"
    }
    r_token = requests.post(token_url, data=token_data, timeout=15)
    if r_token.status_code != 200:
        print(f"❌ Falha ao obter token Azure: {r_token.status_code} - {r_token.text}")
        sys.exit(1)
    
    token = r_token.json().get("access_token")
    print("✅ Token Azure AD obtido com sucesso!")

    print(f"[*] Chamando REST API do Fabric para executar o Pipeline...")
    fabric_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    r_pipe = requests.post(fabric_url, headers=headers, timeout=20)
    print(f"Status Fabric API: {r_pipe.status_code}")
    if r_pipe.status_code in [200, 202]:
        location = r_pipe.headers.get("Location", "")
        print(f"✅ Pipeline iniciado com sucesso no Fabric! Instância: {location}")
        sys.exit(0)
    else:
        print(f"❌ Falha na API do Fabric: {r_pipe.status_code} - {r_pipe.text}")
        sys.exit(1)

print("❌ Nenhuma credencial de autenticação configurada nos Secrets do repositório!")
print("Configure AZURE_CLIENT_ID / AZURE_CLIENT_SECRET / AZURE_TENANT_ID ou FABRIC_WEBHOOK_URL nos Secrets do GitHub.")
sys.exit(1)
