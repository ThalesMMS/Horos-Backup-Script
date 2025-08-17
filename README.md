# Horos → Backup Automático de Exames CT/MR em ZIP (com retomada)

## 📖 Introdução e contexto

Este projeto automatiza o **backup de exames DICOM** armazenados no Horos (visualizador DICOM para macOS), exportando-os em **arquivos `.zip`** organizados por **mês** (`YYYY_MM`).  
Foi pensado para cenários de **alto volume** (dezenas de milhares de estudos; ex.: 2 TB / 50–130 mil exames), onde **exportar manualmente** é inviável e um **Mac antigo** (ex.: Mac mini 2014) precisa ser **poupado**.

**Problemas que motivaram esta solução**:
- Exportação manual é lenta e propensa a erros.
- O Horos pode estar **importando** exames a cada 30 min; não queremos competir I/O nem travar a interface.
- Precisamos de **retomada à prova de quedas** (energia, travamentos).
- Queremos **organização por mês** e **ZIP por estudo**, com nomes úteis (Paciente, DOB, Data do Estudo, UID).

---

## 🧭 O que este projeto faz

- Exporta **apenas CT/MR**, **15 estudos por execução**, **a cada 10 minutos** (configurável).
- Gera **um ZIP por estudo**, com nome: `Paciente_DOB_StudyDate_UID.zip`.
- Organiza os ZIPs em pastas `YYYY_MM` (ex.: `2021_03/`).
- Garante **retomada**: se cair no meio, volta exatamente de onde parou.
- Evita interferir com a importação do Horos: se `INCOMING.noindex` tiver **> 25.000 arquivos**, **pula** o ciclo.
- Verifica **integridade** do ZIP (`testzip()`), rezipa até 3 tentativas; se persistir, registra em `issues.csv`.
- Garante que você **nunca grava** no SSD interno por engano (usa **arquivo sentinela** no volume externo).
- Loga tudo com **rotação** (100 MB × 10).

---

## 🏗️ Fluxo de trabalho (resumo)

1. Checa se o volume `/Volumes/PACS` está **montado** (sentinela `.pacs_sentinel`).  
2. Garante que **não há outra execução** em andamento (file lock).
3. Verifica contagem de arquivos em `INCOMING.noindex`; se **> 25k**, **pula** este ciclo.
4. Remove a **pasta mensal mais recente** caso esteja **incompleta** (sem `.month_done`).
5. Cria uma **cópia consistente** do `Database.sql` (API de **backup** do SQLite).
6. Seleciona os **15 estudos mais antigos** (CT/MR) **ainda não exportados** (ordem estável por data + UID).
7. Para cada estudo: coleta os arquivos `.dcm`, cria **ZIP atômico** (`.part` → rename), roda **`testzip()`** e registra como exportado.
8. Marca os **meses tocados** como concluídos (`.month_done`).

---

## 📂 Estrutura do Backup
```
/Volumes/PACS
├── Database/
│   └── Horos Data/
│       ├── Database.sql
│       └── INCOMING.noindex/
└── Backup/
    ├── horos_backup_export.py
    ├── export_state.sqlite
    ├── issues.csv
    ├── logs/
    │   └── horos_backup.log
    ├── 2021_01/
    │   ├── Paciente1_1980-05-03_2021-01-10_UID123.zip
    │   └── .month_done
    ├── 2021_02/
    │   ├── ...
    └── .tmp/
        ├── dbcopy/Database_copy.sql
        └── .run.lock
```

---

## 🔒 Regras e salvaguardas
- **Volume sentinela**: exige `/Volumes/PACS/.pacs_sentinel`. Sem ele, **aborta** (evita gravar no SSD interno).
- **Lock de execução**: impede rodadas sobrepostas (se uma passar de 10 min, a próxima **espera**).
- **INCOMING.noindex**: se **> 25.000 arquivos**, **pula** a rodada (Horos possivelmente reimportando).
- **Retomada mensal**: se a pasta `YYYY_MM` mais recente **não** tiver `.month_done`, é **apagada** e refeita.
- **ZIP atômico**: escreve `.part` e só depois renomeia para `.zip` (evita ZIPs corrompidos visíveis).
- **Integridade**: `testzip()` após cada export; até **3 tentativas** antes de registrar `ZIP_FAIL`.
- **Nomes únicos**: preserva **UID** integral; truncagem a **128** caracteres; se colidir, sufixos `_2`, `_3`…
- **Estado**: `export_state.sqlite` guarda `studyUID` exportados (não reexporta).

---

## ✅ Requisitos
- macOS (com **launchd** padrão do sistema).
- **Python 3.8+** em `/usr/bin/python3` (sem dependências externas).
- Horos com base de dados em `/Volumes/PACS/Database/Horos Data/`.

---

## 🚀 Instalação

1) **Criar sentinela no volume PACS**
```bash
touch "/Volumes/PACS/.pacs_sentinel"
```

2) **Copiar os arquivos**
```
/Volumes/PACS/Backup/horos_backup_export.py
~/Library/LaunchAgents/com.horos.backup.plist
```

3) **Permissão de execução**
```bash
chmod +x "/Volumes/PACS/Backup/horos_backup_export.py"
```

4) **Carregar o LaunchAgent**
```bash
launchctl load ~/Library/LaunchAgents/com.horos.backup.plist
```

5) **Rodar imediatamente (opcional)**
```bash
launchctl start com.horos.backup
```

---

## 🛠️ Operação e monitoramento

**Logs rotacionados (100 MB × 10):**
```bash
tail -f "/Volumes/PACS/Backup/logs/horos_backup.log"
```

**Logs do launchd:**
```bash
tail -f /tmp/horos_backup_export.out /tmp/horos_backup_export.err
```

**Issues (eventos como NO_FILES, ZIP_FAIL, INCOMING_OVER_LIMIT):**  
`/Volumes/PACS/Backup/issues.csv`

---

## 🔧 Parâmetros úteis (no script)

- **Modalidades**: `MODS = ("CT", "MR")`  
- **Tamanho do lote**: `BATCH_SIZE = 15`  
- **Intervalo entre estudos**: `SLEEP_BETWEEN_STUDIES = 1` (segundos)  
- **Ordenação**: `ORDER_BY = "study_date"` (ou `"date_added"`)  
- **Limiar INCOMING**: `INCOMING_MAX_FILES = 25_000`  
- **Comprimento do nome**: `MAX_NAME_NOEXT = 128`  
- **Logs**: `LOG_MAX_BYTES = 100 * 1024 * 1024`, `LOG_BACKUP_COUNT = 10`

> **Nota**: se trocar `ORDER_BY` para `"date_added"`, a ordenação passa a usar `ZSTUDY.ZDATEADDED ASC, ZSTUDY.ZSTUDYINSTANCEUID ASC`.

---

## 🧪 Teste rápido (apenas 1 ciclo)

> Útil para validar sem esperar o agendamento.

```bash
/usr/bin/python3 "/Volumes/PACS/Backup/horos_backup_export.py"
```

Se quiser reduzir o lote só para o teste, abra o script e mude `BATCH_SIZE = 3` temporariamente.

---

## ❓ Troubleshooting

**Abortou com “sentinela ausente”**  
Crie `/Volumes/PACS/.pacs_sentinel` no volume externo correto.

**Nada exportado; log mostra INCOMING_OVER_LIMIT**  
Horos possivelmente reimportando; aguarde `INCOMING.noindex` cair abaixo de 25k arquivos.

**NO_FILES em `issues.csv`**  
Estudo órfão (paths não encontrados). Verifique integridade/paths no storage do Horos.

**ZIP_FAIL em `issues.csv`**  
Falha após 3 tentativas e `testzip()`. Verifique I/O do disco e permissões.

**Quero mudar para `date_added`**  
Edite `ORDER_BY = "date_added"` e salve.

---

## 📝 Observações de segurança e privacidade
- Os nomes de arquivo incluem **nome do paciente** e **datas**. Avalie políticas internas antes de compartilhar os ZIPs fora do ambiente controlado.
- Não há criptografia em repouso por padrão (foco em performance). Se necessário, considere encriptar o volume APFS.

---

## ✅ Conclusão

Esta automação resolve a necessidade de **backups confiáveis** de grandes repositórios DICOM no Horos,  
com **baixa intervenção**, **resiliência a falhas** e **respeito ao ambiente de importação** do PACS.
