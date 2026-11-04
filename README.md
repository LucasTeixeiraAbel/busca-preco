# Radar de Ofertas

Base para monitoramento de preços com Next.js/PWA e serviços assíncronos FastAPI, Redis Streams e PostgreSQL.

## Serviços locais

1. Copie `.env.example` para `.env`. Para o bot, preencha `TELEGRAM_BOT_TOKEN` e `TELEGRAM_BOT_USERNAME` depois de criá-lo com `@BotFather`.
2. Suba a plataforma local: `docker compose up --build`.
3. Abra `http://localhost:3000`. Para iniciar também o bot Telegram em long polling, use `docker compose --profile telegram up --build`.

O Compose publica o frontend, API, PostgreSQL e Redis apenas em `localhost`; scraper, scheduler e notifier rodam como workers separados. O primeiro build baixa imagens e Chromium do Playwright e precisa de internet. A stack não cria cobranças de nuvem; Docker Desktop pode exigir licença paga conforme o tipo e porte da organização. O Telegram não cobra pelo envio normal de mensagens. O bot não recebe mensagens privadas até cada pessoa abrir o chat e enviar `/start`.

Para rodar sem Docker: instale as dependências Python e o Chromium do Playwright; execute `uvicorn backend.worker:app`, `python -m backend.scraper`, `python -m backend.notify` e, se for usar Telegram, `python -m backend.telegram_bot`. Inicie o frontend com `npm install` e `npm run dev`.

O SQL inicial é aplicado automaticamente somente na primeira criação do volume PostgreSQL. Para volumes existentes, aplique `backend/schema.sql` manualmente com `psql "$DATABASE_URL" -f backend/schema.sql`.

O código mais antigo pode receber `backend/migrations/002_user_alert_auth.sql` depois do schema base: `psql "$DATABASE_URL" -f backend/migrations/002_user_alert_auth.sql`.
Para instalar o catálogo em um volume existente, aplique também `backend/migrations/004_product_catalog.sql`.

## Endpoints

- `POST /scrape`: agenda uma coleta avulsa autenticada com Authorization: Bearer ALERT_ADMIN_TOKEN em `scrape:jobs`, com `{"product_id":"ssd-1tb-kabum","url":"https://www.kabum.com.br/produto/ID"}`. Não precisa enviar preço nem nome de loja; o rótulo é derivado do domínio autorizado. Resposta `202` confirma apenas o enfileiramento.
- `POST /admin/products`: cadastra/atualiza um anúncio para monitoramento recorrente. Envie `product_id`, `title`, `url` e opcionalmente `interval_minutes` (5–10080); exige `Authorization: Bearer <ALERT_ADMIN_TOKEN>`.
- `GET /admin/products`: lista anúncios acompanhados e último preço observado; exige token administrativo.
- `DELETE /admin/products/{product_id}`: pausa um anúncio sem apagar o histórico; exige token administrativo.
- `GET /feed`: entrega os últimos itens do feed quente em Redis.

Hosts adicionais podem ser configurados em `STORE_ALLOWLIST` no `.env`, usando hosts exatos separados por vírgula. Os domínios exatos de Amazon, KaBuM, Casas Bahia, Magazine Luiza e Mercado Livre são habilitados pelos adaptadores internos de descoberta. A descoberta automática vem habilitada por padrão: o scheduler pesquisa um termo por loja a cada 15 minutos e alterna pelos termos de `DISCOVERY_QUERIES` (separados por `|`). Para limitar a carga, cadastra até quatro anúncios por pesquisa e verifica cada anúncio a cada 15 minutos. Ajuste esses limites com `DISCOVERY_INTERVAL_MINUTES`, `DISCOVERY_MAX_RESULTS_PER_QUERY`, `DISCOVERY_OFFER_INTERVAL_MINUTES` e `DISCOVERY_BATCH_SIZE`.

Amazon, KaBuM, Casas Bahia e Magazine Luiza usam suas páginas de busca; o adaptador de busca oficial do Mercado Livre requer `MERCADOLIVRE_ACCESS_TOKEN` de uma aplicação autorizada. Se uma loja negar acesso (por exemplo, HTTP 403), o coletor registra a falha sem repetir a busca por seis horas. Não contornamos bloqueios da loja; fontes que exigem credenciais ou parceria precisam ser habilitadas com autorização válida. O coletor aceita apenas URLs de produto nos domínios exatos suportados e mantém a validação anti-redirect. Produtos são agrupados entre lojas quando a página informa um GTIN/EAN; sem identificador confiável, os anúncios ficam separados para evitar misturar variantes. Preços bugados podem ser detectados pelo histórico da própria loja ou, para produtos com GTIN observados em ao menos duas outras lojas, pela comparação entre lojas. Uma nova oferta precisa de duas leituras confirmatórias antes do alerta.

Para bancos existentes, aplique `backend/migrations/010_catalog_discovery.sql` depois das migrações anteriores. Use `DISCOVERY_ENABLED=false` para pausar a descoberta sem parar o acompanhamento de produtos já cadastrados. Os termos padrão cobrem categorias populares, não todos os produtos existentes no varejo; amplie a cobertura com mais termos separados por `|`.

O worker de scraping aceita HTTPS apenas em domínios da allowlist, carrega a página com Playwright e procura metadados `product:price:amount`, `itemprop=price` ou `twitter:data1`. O rótulo `Amazon`/`KaBuM` é derivado do host; outros hosts autorizados aparecem pelo domínio. Lojas sem metadados compatíveis precisam de adaptadores específicos; scraping genérico não é confiável para todas as páginas.

O detector usa mediana e MAD, grava amostras no PostgreSQL e mantém feed/histórico recente no Redis. O notificator usa `alert_subscribers` e `push_subscriptions`; o provisionamento dessas tabelas deve ser feito por fluxo autenticado e consentido antes de produção. Não exponha cadastro de e-mail sem confirmação e autenticação. Chaves VAPID e credenciais dos provedores devem ficar em secret manager, nunca no repositório.

## Provisionamento administrativo de alertas

Configure `ALERT_ADMIN_TOKEN` com pelo menos 32 caracteres aleatórios. As rotas administrativas exigem `Authorization: Bearer <token>` e têm limite Redis por IP:

- `PUT /admin/alert-subscribers` com `{"email":"pessoa@example.com"}` habilita um destinatário.
- `DELETE /admin/alert-subscribers/{email}` desabilita o destinatário.
- `PUT /admin/push-subscriptions` recebe o objeto `PushSubscription` do navegador (`endpoint` e `keys`) e aceita somente hosts HTTPS de `PUSH_ENDPOINT_HOSTS`.
- `DELETE /admin/push-subscriptions?endpoint=<URL>` desabilita uma subscription.

Gere o token com `python -c "import secrets; print(secrets.token_urlsafe(48))"` e mantenha-o apenas em sistemas confiáveis de operação. **Não coloque esse token no Next.js ou no navegador.**

## Login e consentimento de alertas

Configure `AUTH_JWT_SECRET` com outro segredo aleatório de pelo menos 32 caracteres, `FRONTEND_ORIGIN` para a origem pública do Next.js e `NEXT_PUBLIC_VAPID_PUBLIC_KEY` com a chave pública VAPID correspondente à chave privada do worker. O usuário informa o e-mail e recebe um link de uso único válido por 15 minutos. A validação cria sessão de uma hora em cookie HttpOnly, Secure em produção e SameSite=Lax; o Next.js encaminha as chamadas privadas ao backend sem expor o JWT ou o token administrativo ao navegador.

O usuário precisa marcar o consentimento de e-mail e pode ativar push por dispositivo. Desativar push remove a subscription daquele aparelho. Os links mágicos são enviados pelo Resend, SES ou SendGrid; para Resend, use `onboarding@resend.dev` em testes locais; em produ??o, configure um `ALERT_FROM_EMAIL` de dom?nio verificado. Os limites de login são 10 tentativas por IP a cada 15 minutos e 3 por endereço a cada hora.

Para vincular o Telegram, a pessoa autenticada solicita o link no painel, abre o bot e pressiona **Start**. O link expira em 10 minutos e só funciona uma vez. A mensagem `/stop` desativa os alertas daquele chat; o painel também pode desconectá-lo. Execute a migração `backend/migrations/003_telegram_alerts.sql` em bancos que já existiam antes desta etapa. O bot deve usar long polling em apenas uma instância e não pode compartilhar o token com outro processo de polling/webhook. Se já houver webhook configurado no bot, remova-o conscientemente antes de iniciar o serviço de long polling.

## Limites e operação

Os limites por IP são Redis-backed, porém dependem do IP de conexão. Atrás de proxy, confie somente em proxies conhecidos e não aceite `X-Forwarded-For` arbitrário. A allowlist reduz risco de SSRF no coletor; não permita domínio configurável pelo usuário. Redis mantém cache e filas, PostgreSQL mantém histórico durável. Para escalar, execute réplicas dos consumidores com IDs únicos e monitore mensagens pendentes, idade da fila e taxa de erros. O envio ao provedor pode ocasionalmente duplicar se o processo cair após o envio e antes do ACK; provedores/consumidores de produção devem tratar idempotência e retries.

O PWA exibe notificações recebidas pelo service worker. A meta de entrega inferior a cinco segundos depende da latência da loja e dos provedores externos, e precisa ser medida em ambiente implantado.
