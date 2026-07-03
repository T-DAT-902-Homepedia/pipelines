"""Tests du parsing des avis (pur, hors-ligne).

Vérifie surtout les deux corrections de bugs vs l'ancien scrape_top80 :
le DERNIER avis de page n'est plus perdu, et TOUTES les notes de catégorie
sont captées (pas seulement la première ligne du tableau).
"""

from homepedia_ville_ideale.scraper import (
    CATEGORY_MAP,
    page_url,
    parse_reviews,
    parse_ville_name,
)

# Deux avis : le second n'est PAS suivi d'un autre <div class="comm"> (il ferme
# la zone). L'ancienne regex le perdait.
HTML = """
<h1>Nice (06000)</h1>
<section id="commentaires">
<div class="comm">
  <p>Avis posté le 15-03-2024 à 10h. Par <strong>SecretPseudo42</strong></p>
  <p class="moyenne">7,50</p>
  <table>
    <tr><th>Environnement</th><td>8</td></tr>
    <tr><th>Transports</th><td>6</td></tr>
    <tr><th>Sécurité</th><td>4</td></tr>
  </table>
  <p><b>Points positifs</b> Ville calme et agréable, commerces variés.
     <b>Points négatifs</b> Stationnement compliqué le soir.</p>
</div>
<div class="comm">
  <p>Avis posté le 20-11-2023 à 09h. Par <strong>DernierAvis</strong></p>
  <p class="moyenne">4,00</p>
  <table>
    <tr><th>Environnement</th><td>5</td></tr>
    <tr><th>Commerces</th><td>7</td></tr>
  </table>
  <p><b>Points positifs</b> Le marché est vivant.
     <b>Points négatifs</b> Insécurité en hausse.</p>
</div>
</section>
"""


def test_parses_both_reviews_including_last():
    reviews = parse_reviews(HTML, "nice", "06088", "Nice")
    assert len(reviews) == 2  # le dernier avis n'est plus perdu
    assert reviews[1]["auteur"] == "DernierAvis"


def test_parses_all_category_notes_not_just_first():
    reviews = parse_reviews(HTML, "nice", "06088", "Nice")
    first = reviews[0]
    # les 3 notes du tableau sont toutes captées (bug : seule la 1ʳᵉ l'était)
    assert first["environnement"] == "8"
    assert first["transports"] == "6"
    assert first["securite"] == "4"


def test_parses_texts_and_metadata():
    review = parse_reviews(HTML, "nice", "06088", "Nice")[0]
    assert review["note_moyenne"] == "7.50"
    assert review["date"] == "15-03-2024"
    assert "calme" in review["points_positifs"].lower()
    assert "stationnement" in review["points_negatifs"].lower()


def test_review_has_all_columns():
    review = parse_reviews(HTML, "nice", "06088", "Nice")[0]
    for key in ("slug", "code_commune", "nom_ville", *CATEGORY_MAP.values()):
        assert key in review


def test_parse_ville_name_strips_population():
    assert parse_ville_name("<h1>Nice (06000)</h1>") == "Nice"


def test_skips_blocks_without_rating():
    html = '<div class="comm"><p>Un texte sans note.</p></div>'
    assert parse_reviews(html, "x", "00000", "X") == []


def test_page_url():
    assert page_url("nice", "06088", 1) == "https://www.ville-ideale.fr/nice_06088"
    assert "page=2" in page_url("nice", "06088", 2)


def _load_communes():
    # Le driver n'est pas un module importable : on charge la fonction par chemin.
    import importlib.util
    from pathlib import Path

    path = Path(__file__).parent.parent / "scripts" / "scrape_ville_ideale.py"
    spec = importlib.util.spec_from_file_location("scrape_driver", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.load_communes


def test_load_communes_strips_code_suffix_from_slug(tmp_path):
    load_communes = _load_communes()
    csv_path = tmp_path / "communes.csv"
    csv_path.write_text(
        "slug,code_commune\n"
        "amberieu-en-bugey_1004,1004\n"  # slug suffixé + code 4 chars
        "nice,06088\n",  # slug nu
        encoding="utf-8",
    )
    communes = load_communes(csv_path)
    assert communes == [("amberieu-en-bugey", "01004"), ("nice", "06088")]
