#!/bin/sh
# =============================================================================
# Provisionamento automatico do Metabase (Etapa 6).
# Idempotente: pode rodar varias vezes sem duplicar nada.
#   1. aguarda /api/health
#   2. setup (admin + data source) se a instancia for nova (setup-token)
#   3. login -> sessao
#   4. descobre o id do data source "Gold (destino)"
#   5. se o dashboard ainda nao existir, cria 6 cards e o dashboard
# Requisitos no container: curl, jq. Variaveis: MB_URL, MB_ADMIN_EMAIL,
# MB_ADMIN_PASSWORD, DEST_DB_HOST/PORT/NAME/USER/PASSWORD/SSLMODE.
# =============================================================================
set -eu

MB_URL="${MB_URL:-http://metabase:3000}"
SITE_NAME="Data Pipeline"
DB_DISPLAY_NAME="Gold (destino)"
DASHBOARD_NAME="Pipeline — Vendas"

log() { echo "[metabase-init] $*"; }

# 1. Aguarda o Metabase ficar de pe ----------------------------------------
log "Aguardando o Metabase em ${MB_URL} ..."
until curl -sf "${MB_URL}/api/health" >/dev/null 2>&1; do
  log "ainda nao saudavel; aguardando 3s..."
  sleep 3
done
log "Metabase saudavel."

# Deriva o flag ssl a partir do sslmode do destino
case "${DEST_DB_SSLMODE:-require}" in
  disable|allow|prefer) DEST_SSL=false ;;
  *)                    DEST_SSL=true  ;;
esac

# 2. Setup (idempotente) ----------------------------------------------------
SETUP_TOKEN=$(curl -sf "${MB_URL}/api/session/properties" | jq -r '.["setup-token"] // empty')

if [ -n "${SETUP_TOKEN}" ]; then
  log "Instancia nova: executando setup (admin + data source)..."
  SETUP_PAYLOAD=$(jq -n \
    --arg token "$SETUP_TOKEN" \
    --arg email "$MB_ADMIN_EMAIL" \
    --arg password "$MB_ADMIN_PASSWORD" \
    --arg site "$SITE_NAME" \
    --arg dbdisp "$DB_DISPLAY_NAME" \
    --arg host "$DEST_DB_HOST" \
    --arg port "$DEST_DB_PORT" \
    --arg dbname "$DEST_DB_NAME" \
    --arg user "$DEST_DB_USER" \
    --arg pass "$DEST_DB_PASSWORD" \
    --argjson ssl "$DEST_SSL" \
    '{
      token: $token,
      user: {email: $email, password: $password,
             first_name: "Admin", last_name: "Pipeline", site_name: $site},
      prefs: {site_name: $site, allow_tracking: false},
      database: {
        engine: "postgres",
        name: $dbdisp,
        details: {host: $host, port: ($port|tonumber), dbname: $dbname,
                  user: $user, password: $pass, ssl: $ssl}
      }
    }')
  curl -sf -X POST "${MB_URL}/api/setup" \
    -H "Content-Type: application/json" \
    -d "$SETUP_PAYLOAD" >/dev/null
  log "Setup concluido."
else
  log "Instancia ja configurada: pulando setup."
fi

# 3. Login ------------------------------------------------------------------
log "Autenticando como admin..."
SESSION=$(curl -sf -X POST "${MB_URL}/api/session" \
  -H "Content-Type: application/json" \
  -d "$(jq -n --arg u "$MB_ADMIN_EMAIL" --arg p "$MB_ADMIN_PASSWORD" \
        '{username:$u, password:$p}')" \
  | jq -r '.id // empty')

if [ -z "$SESSION" ]; then
  log "ERRO: falha ao autenticar."
  exit 1
fi
AUTH="X-Metabase-Session: ${SESSION}"

# 4. Descobre o id do data source ------------------------------------------
DB_ID=$(curl -sf -H "$AUTH" "${MB_URL}/api/database" \
  | jq -r --arg name "$DB_DISPLAY_NAME" \
      '(.data // .) | map(select(.name == $name)) | .[0].id // empty')

if [ -z "$DB_ID" ]; then
  log "ERRO: data source '${DB_DISPLAY_NAME}' nao encontrado."
  exit 1
fi
log "Data source id = ${DB_ID}."

# 5. Idempotencia: dashboard ja existe? ------------------------------------
EXISTING=$(curl -sf -H "$AUTH" "${MB_URL}/api/dashboard" \
  | jq -r --arg name "$DASHBOARD_NAME" \
      '(.data // .) | map(select(.name == $name)) | .[0].id // empty')

if [ -n "$EXISTING" ]; then
  log "Dashboard '${DASHBOARD_NAME}' ja existe (id ${EXISTING}). Nada a fazer."
  exit 0
fi

# Cria um card nativo e ecoa o id retornado.
# $1=nome  $2=display  $3=sql
create_card() {
  curl -sf -X POST "${MB_URL}/api/card" \
    -H "$AUTH" -H "Content-Type: application/json" \
    -d "$(jq -n \
          --arg name "$1" --arg display "$2" --arg sql "$3" --argjson db "$DB_ID" \
          '{
            name: $name,
            display: $display,
            visualization_settings: {},
            dataset_query: {type: "native", native: {query: $sql}, database: $db}
          }')" \
    | jq -r '.id'
}

log "Criando cards..."
KPI1=$(create_card "Faturamento total" "scalar" \
  "SELECT SUM(f.quantidade * f.preco) AS faturamento_total FROM fato_vendas f;")
KPI2=$(create_card "Total de pedidos" "scalar" \
  "SELECT COUNT(DISTINCT f.pedido_id) AS total_pedidos FROM fato_vendas f;")
KPI3=$(create_card "Ticket medio por pedido" "scalar" \
  "SELECT SUM(f.quantidade * f.preco) / NULLIF(COUNT(DISTINCT f.pedido_id), 0) AS ticket_medio FROM fato_vendas f;")
KPI4=$(create_card "Itens vendidos" "scalar" \
  "SELECT SUM(f.quantidade) AS itens_vendidos FROM fato_vendas f;")
MET1=$(create_card "Faturamento por mes" "line" \
  "SELECT date_trunc('month', d.data) AS mes, SUM(f.quantidade * f.preco) AS faturamento FROM fato_vendas f JOIN dim_data d ON d.data = date(f.data_pedido) GROUP BY 1 ORDER BY 1;")
MET2=$(create_card "Top 10 produtos por faturamento" "bar" \
  "SELECT p.nome AS produto, SUM(f.quantidade * f.preco) AS faturamento FROM fato_vendas f JOIN dim_produto p ON p.id_produto = f.produto_id AND p.is_current = true GROUP BY 1 ORDER BY 2 DESC LIMIT 10;")

log "Criando dashboard '${DASHBOARD_NAME}'..."
DASH_ID=$(curl -sf -X POST "${MB_URL}/api/dashboard" \
  -H "$AUTH" -H "Content-Type: application/json" \
  -d "$(jq -n --arg name "$DASHBOARD_NAME" '{name:$name}')" \
  | jq -r '.id')

log "Adicionando cards ao dashboard..."
DASHCARDS=$(jq -n \
  --argjson k1 "$KPI1" --argjson k2 "$KPI2" --argjson k3 "$KPI3" --argjson k4 "$KPI4" \
  --argjson m1 "$MET1" --argjson m2 "$MET2" \
  '[
    {id:-1, card_id:$k1, row:0, col:0,  size_x:6,  size_y:3},
    {id:-2, card_id:$k2, row:0, col:6,  size_x:6,  size_y:3},
    {id:-3, card_id:$k3, row:0, col:12, size_x:6,  size_y:3},
    {id:-4, card_id:$k4, row:0, col:18, size_x:6,  size_y:3},
    {id:-5, card_id:$m1, row:3, col:0,  size_x:12, size_y:6},
    {id:-6, card_id:$m2, row:3, col:12, size_x:12, size_y:6}
  ]')

curl -sf -X PUT "${MB_URL}/api/dashboard/${DASH_ID}" \
  -H "$AUTH" -H "Content-Type: application/json" \
  -d "$(jq -n --argjson dc "$DASHCARDS" '{dashcards: $dc}')" >/dev/null

log "Dashboard provisionado com sucesso (id ${DASH_ID})."
