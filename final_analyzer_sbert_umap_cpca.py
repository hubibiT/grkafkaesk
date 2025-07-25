# final_complete_conceptual_analysis_v2.py

import pandas as pd
import re
import numpy as np
from sentence_transformers import SentenceTransformer
import umap
import plotly.express as px
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
import seaborn as sns
import matplotlib.pyplot as plt
import os
import warnings

# ==============================================================================
# KONFIGURATION
# ==============================================================================
REVIEWS_FILENAME = 'goodreads_reviews_english_clean.csv'
SUMMARY_FILENAME = 'goodreads_book_summary_enriched.csv'
EMBEDDINGS_FILENAME_CLEANED = 'sbert_embeddings_mpnet_cleaned_final.npy' 
SBERT_MODEL_NAME = 'all-mpnet-base-v2'

# ==============================================================================
# HELPER-FUNKTIONEN
# ==============================================================================
def remove_specific_entities(text, entities):
    """
    Entfernt eine Liste von Wörtern (case-insensitive) aus einem Text.
    Ihre verbesserte Funktion.
    """
    if not isinstance(text, str):
        return ""
    for entity in entities:
        # \b für Wortgrenzen, um Teilwörter zu schützen
        pattern = r'\b' + re.escape(entity) + r'\b'
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)
    # Entfernt doppelte Leerzeichen und Leerzeichen am Anfang/Ende
    return ' '.join(text.split())

def standardize_join_key(text):
    if not isinstance(text, str): return ""
    return text.lower().strip()

# ==============================================================================
# HAUPTANALYSE
# ==============================================================================
if __name__ == '__main__':
    warnings.simplefilter(action='ignore', category=FutureWarning)
    os.environ["TOKENIZERS_PARALLELISM"] = "false"

    print("--- Finale konzeptuelle Analyse-Pipeline ---")

    # --- 1. LADEN UND ZUSAMMENFÜHREN DER DATEN ---
    try:
        print(f"\n[Schritt 1/5] Lade und verbinde Daten...")
        reviews_df = pd.read_csv(REVIEWS_FILENAME)
        summary_df = pd.read_csv(SUMMARY_FILENAME)
        reviews_df['join_key'] = reviews_df['book_name'].apply(standardize_join_key)
        summary_df['join_key'] = summary_df['book_name'].apply(standardize_join_key)
        metadata_df = summary_df[['join_key', 'author']].drop_duplicates(subset=['join_key'])
        df = pd.merge(reviews_df, metadata_df, on='join_key', how='left')
        df.dropna(subset=['context', 'author', 'date', 'book_name', 'stars'], inplace=True)
        df = df.drop(columns=['join_key'])
        print(f"{len(df)} Reviews erfolgreich geladen.")
    except FileNotFoundError as e:
        print(f"KRITISCHER FEHLER: Eine benötigte Eingabedatei wurde nicht gefunden: {e}")
        exit()

    # --- 2. DATENAUFBEREITUNG ---
    print("\n[Schritt 2/5] Bereite Daten für die Analyse vor...")
    df['is_kafka_author'] = df['author'].apply(lambda s: 'Franz Kafka' if 'kafka' in str(s).lower() else 'Other')
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df.dropna(subset=['date'], inplace=True)
    split_date = '2021-01-01'
    df['period'] = '2021+'
    df.loc[df['date'] < split_date, 'period'] = 'Pre-2021'
    print("Datenaufbereitung abgeschlossen.")

    # --- 3. ENTFERNE SPEZIFISCHE EIGENNAMEN (Proper Nouns) ---
    print("\n[Schritt 3/5] Entferne Buchtitel und Protagonisten-Namen aus den Reviews...")
    entities_to_remove = [
        'metamorphosis', 'gregor', 'samsa', 'the trial', 'josef k', 'joseph k',
        'the castle', 'in the penal colony', 'a hunger artist', 'the judgement',
        'amerika', 'catch-22', 'catch 22', 'murakami', 'saramago', 'blindness', 'camus', 
        'meursault', 'the stranger'
    ]
    df['context_cleaned'] = df['context'].apply(lambda x: remove_specific_entities(x, entities_to_remove))
    print("Eigennamen für die semantische Analyse entfernt.")
    
    # --- 4. SBERT EMBEDDINGS ERSTELLEN (auf den bereinigten Daten) ---
    print(f"\n[Schritt 4/5] Erstelle oder lade SBERT Embeddings für die bereinigten Daten...")
    sentences = df['context_cleaned'].tolist()
    if os.path.exists(EMBEDDINGS_FILENAME_CLEANED):
        print(f"Lade existierende bereinigte Embeddings aus '{EMBEDDINGS_FILENAME_CLEANED}'...")
        embeddings = np.load(EMBEDDINGS_FILENAME_CLEANED)
    else:
        print(f"Generiere neue Embeddings für die bereinigten Daten...")
        model = SentenceTransformer(SBERT_MODEL_NAME)
        embeddings = model.encode(sentences, show_progress_bar=True)
        np.save(EMBEDDINGS_FILENAME_CLEANED, embeddings)
        print(f"Speichere bereinigte Embeddings in '{EMBEDDINGS_FILENAME_CLEANED}'...")
    print(f"Embeddings geladen. Shape: {embeddings.shape}")

    # --- 5. FINALE VISUALISIERUNGEN (auf den bereinigten, konzeptuellen Daten) ---
    print("\n[Schritt 5/5] Erstelle die finalen vier Visualisierungen...")

    reducer_2d = umap.UMAP(n_neighbors=15, min_dist=0.1, n_components=2, random_state=42)
    embeddings_2d = reducer_2d.fit_transform(embeddings)
    df['umap_x'] = embeddings_2d[:, 0]
    df['umap_y'] = embeddings_2d[:, 1]
    df['context_short'] = df['context'].str[:150] + '...'

    # --- VISUALISIERUNG 1 & 2: KAFKA vs. NON-KAFKA ---
    fig_umap_author = px.scatter(
        df, x='umap_x', y='umap_y', color='is_kafka_author',
        hover_data=['book_name', 'author', 'stars', 'period', 'context_short'],
        title='Semantische Karte (Konzepte): Kafka vs. Andere Autoren',
        labels={'color': 'Autoren-Typ', 'umap_x': '', 'umap_y': ''},
        color_discrete_map={'Franz Kafka': '#ff7f0e', 'Other': '#1f77b4'}
    )
    # --- PRÄZISE STEUERUNG DES LAYOUTS ---
    fig_umap_author.update_layout(
        font_family="Arial", 
        title_x=0.5,
        plot_bgcolor='white',  # Setzt den Hintergrund auf weiß
        xaxis=dict(showgrid=False, showline=True, linecolor='lightgrey'), # Entfernt Gitter, zeigt Achsenlinie
        yaxis=dict(showgrid=False, showline=True, linecolor='lightgrey')  # Entfernt Gitter, zeigt Achsenlinie
    )
    fig_umap_author.update_traces(marker=dict(size=4, opacity=0.7))
    fig_umap_author.write_html("final_map_concepts_kafka_vs_other.html")
    print("-> Karte 1 gespeichert.")

    mask_kafka = df['is_kafka_author'] == 'Franz Kafka'
    clf_author = LinearDiscriminantAnalysis()
    clf_author.fit(embeddings, mask_kafka)
    df['LDA_score_author'] = clf_author.transform(embeddings)
    plt.figure(figsize=(12, 7))
    sns.kdeplot(data=df, x='LDA_score_author', hue='is_kafka_author', fill=True, common_norm=False)
    plt.title('LDA (Konzepte): Semantischer Kontrast Kafka vs. Andere Autoren', fontsize=16)
    plt.xlabel('Projektion auf die "maximale Trennungsachse"')
    sns.despine()
    plt.savefig("final_lda_concepts_kafka_vs_other.png", dpi=300)
    print("-> LDA-Plot 1 gespeichert.")
    plt.close()

    # --- VISUALISIERUNG 3 & 4: PRE- vs. POST-2021 ---
    fig_umap_period = px.scatter(
        df, x='umap_x', y='umap_y', color='period',
        hover_data=['book_name', 'author', 'stars', 'period', 'context_short'],
        title='Semantische Karte (Konzepte): Reviews vor vs. nach 2021',
        labels={'color': 'Zeitperiode', 'umap_x': '', 'umap_y': ''},
        color_discrete_map={'Pre-2021': '#1f77b4', '2021+': '#ff7f0e'}
    )
    # --- PRÄZISE STEUERUNG DES LAYOUTS ---
    fig_umap_period.update_layout(
        font_family="Arial", 
        title_x=0.5,
        plot_bgcolor='white',  # Setzt den Hintergrund auf weiß
        xaxis=dict(showgrid=False, showline=True, linecolor='lightgrey'), # Entfernt Gitter, zeigt Achsenlinie
        yaxis=dict(showgrid=False, showline=True, linecolor='lightgrey')  # Entfernt Gitter, zeigt Achsenlinie
    )
    fig_umap_period.update_traces(marker=dict(size=4, opacity=0.7))
    fig_umap_period.write_html("final_map_concepts_pre_vs_post_2021.html")
    print("-> Karte 2 gespeichert.")

    mask_pre_2021 = df['period'] == 'Pre-2021'
    clf_period = LinearDiscriminantAnalysis()
    clf_period.fit(embeddings, mask_pre_2021)
    df['LDA_score_period'] = clf_period.transform(embeddings)
    plt.figure(figsize=(12, 7))
    sns.kdeplot(data=df, x='LDA_score_period', hue='period', fill=True, common_norm=False)
    plt.title('LDA (Konzepte): Semantischer Wandel vor vs. nach 2021', fontsize=16)
    plt.xlabel('Projektion auf die "maximale Trennungsachse"')
    sns.despine()
    plt.savefig("final_lda_concepts_pre_vs_post_2021.png", dpi=300)
    print("-> LDA-Plot 2 gespeichert.")
    plt.close()

    print("\n\n--- Analyse vollständig abgeschlossen! Alle vier konzeptuellen Artefakte wurden generiert. ---")

if __name__ == '__main__':
    main()