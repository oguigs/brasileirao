## Deploy no Databricks (CI/CD)

Os jobs são definidos como código em `resources/jobs/` e publicados no Databricks com **Databricks Asset Bundles**. A cada push na `main`, o GitHub Actions (`.github/workflows/deploy-databricks.yml`) faz o deploy dos jobs e dos notebooks de `src/notebooks/`. Em pull requests, o workflow só valida o bundle.

### Configuração (uma vez por repositório)

**1. Gerar um token no Databricks**

No Databricks, clique no avatar (canto superior direito) → **Settings → Developer → Access tokens → Manage → Generate new token**. Dê um nome (ex.: `github-actions`) e copie o valor, que só aparece uma vez.

**2. Cadastrar os secrets no GitHub**

No repositório: **Settings → Secrets and variables → Actions → New repository secret**. Crie dois:

| Name | Valor |
|---|---|
| `DATABRICKS_HOST` | URL do workspace, até o `.com`, sem barra no final. Ex.: `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Token gerado no passo 1 |

Use **Repository secrets** (não *Environment secrets*).

**3. Fazer o primeiro deploy**

Faça push na `main` (ou rode o workflow manualmente em **Actions → Deploy Databricks → Run workflow**). Quando o workflow terminar verde, os jobs aparecem em **Jobs & Pipelines** no Databricks.

**4. Remover jobs antigos criados pela UI**

O bundle cria jobs novos e não adota os existentes. Apague qualquer job `brasileirao_*` criado manualmente, senão o gatilho de arquivo e o agendamento disparam em dobro.

### Regras de uso

- **Não edite os jobs pela UI do Databricks.** O próximo deploy sobrescreve as alterações. Mude o YAML em `resources/jobs/` e faça push.
- Todo arquivo em `src/notebooks/` usado como notebook precisa começar com a linha `# Databricks notebook source`.
- Os caminhos dos notebooks nos YAMLs são relativos ao próprio YAML (ex.: `../../src/notebooks/20_silver_partidas.py`).

### Deploy local (opcional)

Com a [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) instalada e autenticada (`databricks auth login`):

```bash
databricks bundle validate
databricks bundle deploy -t prod
```

### Segurança

- Os secrets ficam criptografados nas configurações do GitHub e **não** vão junto com clone ou fork. Workflows de PR vindos de fork não recebem secrets.
- Nunca commite tokens. `.databrickscfg`, `.env` e `.databricks/` estão no `.gitignore`.
- Se um token vazar, revogue em **Settings → Developer → Access tokens** e atualize o secret `DATABRICKS_TOKEN`.