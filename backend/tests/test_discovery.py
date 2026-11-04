from backend.discovery import canonical_url, extract_page_products, product_key, search_marketplace, search_url
from backend.worker import detect_cross_store_anomaly


def test_extract_products_uses_structured_data_and_normalizes_tracking_url():
    html = '''
    <script type="application/ld+json">
      {"@context":"https://schema.org","@type":"ItemList","itemListElement":[
        {"@type":"Product","name":"Console Example 1TB","gtin13":"7891234567895",
         "url":"https://www.magazineluiza.com.br/console-example/p/123/?utm_source=ad"}
      ]}
    </script>
    <a href="https://evil.example/product/p/123">bad result</a>
    '''
    products = extract_page_products(
        html, "https://www.magazineluiza.com.br/busca/console/", "magalu", limit=5,
    )
    assert products == [{
        "url": "https://www.magazineluiza.com.br/console-example/p/123",
        "title": "Console Example 1TB",
        "gtin": "7891234567895",
    }]


def test_extract_products_falls_back_only_to_product_shaped_store_links():
    html = '''
      <a href="/produto/123/placa-de-video">Placa de vídeo RTX 9999</a>
      <a href="/busca/placa-de-video">Ver mais resultados</a>
      <a href="https://www.casasbahia.com.br/produto/p/991">other retailer</a>
    '''
    products = extract_page_products(html, "https://www.kabum.com.br/busca/placa", "kabum", limit=5)
    assert len(products) == 1
    assert products[0]["url"] == "https://www.kabum.com.br/produto/123/placa-de-video"


def test_canonical_url_rejects_external_hosts_and_removes_query_parameters():
    assert canonical_url(
        "https://www.casasbahia.com.br/produto/p/123?utm_campaign=x",
        "https://www.casasbahia.com.br/busca/tv", "casasbahia",
    ) == "https://www.casasbahia.com.br/produto/p/123"
    assert canonical_url("https://attacker.example/produto/p/1", "https://x/", "casasbahia") is None
    assert canonical_url(
        "https://www.casasbahia.com.br/produto/p/123?sku=blue&utm_source=ad",
        "https://www.casasbahia.com.br/busca/tv", "casasbahia",
    ) == "https://www.casasbahia.com.br/produto/p/123?sku=blue"


def test_product_key_only_cross_merges_with_gtin_or_marketplace_catalog_id():
    assert product_key("kabum", "https://www.kabum.com.br/produto/1", "7891234567895") == "gtin:7891234567895"
    assert product_key("mercadolivre", "https://www.mercadolivre.com.br/p/MLB1", external_id="MLB123") == "mercadolivre:MLB123"
    assert product_key("magalu", "https://www.magazineluiza.com.br/x/p/1") != product_key(
        "casasbahia", "https://www.casasbahia.com.br/x/p/1",
    )


def test_store_search_url_encodes_query_terms():
    assert search_url("kabum", "smart tv") == "https://www.kabum.com.br/busca/smart-tv"


def test_marketplace_search_uses_configured_oauth_token(monkeypatch):
    import asyncio

    monkeypatch.setenv("MERCADOLIVRE_ACCESS_TOKEN", "test-token")

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return {"results": [{
                "id": "MLB1234567890",
                "catalog_product_id": "MLB987654",
                "title": "Console Example 1TB",
                "permalink": "https://www.mercadolivre.com.br/console/p/MLB987654",
                "attributes": [{"id": "GTIN", "value_name": "7891234567895"}],
            }]}

    class Client:
        async def get(self, url, *, params, headers):
            assert url == "https://api.mercadolibre.com/sites/MLB/search"
            assert params["q"] == "console"
            assert headers["Authorization"] == "Bearer test-token"
            return Response()

    items = asyncio.run(search_marketplace(Client(), "console"))
    assert items[0]["gtin"] == "7891234567895"
    assert items[0]["external_id"] == "MLB987654"


def test_cross_store_anomaly_requires_two_comparable_stores_and_25_percent_drop():
    comparable = [
        {"store": "KaBuM", "price": 3000},
        {"store": "Magazine Luiza", "price": 3100},
    ]
    assert detect_cross_store_anomaly(comparable, 2200) == (True, -0.2786885245901639)
    assert detect_cross_store_anomaly(comparable[:1], 1000) == (False, 0.0)
    assert detect_cross_store_anomaly(comparable, 2500) == (False, -0.18032786885245902)
