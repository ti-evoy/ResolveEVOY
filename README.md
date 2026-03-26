<<<<<<< HEAD
# resolveEvoy
=======
# ResolvEVOY

Aplicativo Streamlit para cadastro, busca, edicao e dashboard de reclamacoes.
Agora ele esta configurado para rodar online usando Google Sheets como base de dados.

Recursos incluidos:

- protocolo automatico
- status do caso
- responsavel interno
- data de retorno
- filtros de pendencia
- log de alteracoes no proprio registro

## Como rodar

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run app.py
```

## Configuracao

Os segredos do Streamlit ficam em `.streamlit/secrets.toml` com:

- `sheet_id`
- `access_token`
- `gcp_service_account`

## Importante

- A planilha precisa estar compartilhada com o e-mail da conta de servico
- O app usa a aba `registros` para os chamados
- O app usa a aba `acessos` para login e perfil dos usuarios
- Se a planilha estiver vazia, o cabecalho e criado automaticamente
>>>>>>> 7921f62 (melhorias de segurança)
