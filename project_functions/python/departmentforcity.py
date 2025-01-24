import pandas as pd

# Data for communes and their corresponding departments (mock data for this example)
data = {
    "municipality": [
        'Anthenaise', 'Auvers-sur-Oise', 'Bardelle', 'Belfonds', 'Bertangles',
        'Betton', 'Bondy', 'Bordeaux', 'Boucoiran-et-Nozières', 'Boulogne',
        'Brassac', 'Brizon', 'Cabrières', 'Camprond', 'Carcès',
        'Chamarandes-Choignes', 'Champniers', 'Chevigney-lès-Vercel',
        'Châtillon', 'Chèvremont', 'Clacy-et-Thierret',
        'Collonge-en-Charollais', 'Corrèze', 'Coulaines', 'Coutiches',
        'Couzeix', 'Créteil', 'Digne-les-Bains', 'Dognen', 'Feldkirch',
        'Finestret', 'Fitz-James', 'Fontaine-lès-Boulans', 'Gerde',
        'Granges-sur-Lot', 'Guessling-Hémering', 'La Chaize-le-Vicomte',
        'Ladern-sur-Lauquet', 'Lardy', 'Longages', 'Lopérec', 'Malleloy',
        'Marre', 'Massoins', 'May-sur-Orne', 'Mazerny', 'Mazières-en-Gâtine',
        'Mende', 'Montigny-lès-Vesoul', 'Monéteau', 'Moustoir-Ac',
        "Moutier-d'Ahun", 'Mûrs-Erigné', 'Niherne', 'Nogent-sur-Eure',
        'Notre-Dame-de-Sanilhac', 'Notre-Dame-des-Landes', 'Ordan-Larroque',
        'Osmoy', 'Paris', 'Plaine-Haute', 'Poncins', 'Pont-de-Salars',
        'Pérignat-lès-Sarliève', 'Quiers', 'Quittebeuf', 'Réalmont',
        'Récourt-le-Creux', 'Saint-Chamas', 'Saint-Claude-de-Diray',
        'Saint-Georges-des-Coteaux', 'Saint-Jean-Saint-Nicolas',
        'Saint-Joseph-des-Bancs', "Saint-Julien-l'Ars", "Saint-Martin-d'Abbat",
        'Saint-Martin-du-Mont(Ain)', "Saint-Martin-du-Mont(Cote d'Or)",
        'Saint-Paulien', 'Saint-Pierre', 'Saint-Saulge', 'Saint-Sauveur-en-Diois',
        'Santa-Maria-Siché', 'Schnersheim', 'Sorigny', 'Soulomès', 'Thiézac',
        'Treban', 'Uxegney', 'Valmorel', 'Varneville-Bretteville', 'Venasque',
        'Veurey-Voroize', 'Villechétif', 'Villemade', 'Villenave', 'Érone'
    ],
    "dep_name_upper": [
        'Mayenne', 'Val-d\'Oise', 'Haute-Marne', 'Orne', 'Somme', 'Ille-et-Vilaine',
        'Seine-Saint-Denis', 'Gironde', 'Gard', 'Pas-de-Calais', 'Tarn',
        'Haute-Savoie', 'Hérault', 'Manche', 'Var', 'Haute-Marne', 'Charente',
        'Doubs', 'Hauts-de-Seine', 'Territoire de Belfort', 'Aisne',
        'Saône-et-Loire', 'Corrèze', 'Sarthe', 'Nord', 'Haute-Vienne',
        'Val-de-Marne', 'Alpes-de-Haute-Provence', 'Pyrénées-Atlantiques',
        'Haut-Rhin', 'Pyrénées-Orientales', 'Oise', 'Pas-de-Calais', 'Hautes-Pyrénées',
        'Lot-et-Garonne', 'Moselle', 'Vendée', 'Aude', 'Essonne', 'Haute-Garonne',
        'Finistère', 'Meurthe-et-Moselle', 'Meuse', 'Alpes-Maritimes', 'Calvados',
        'Ardennes', 'Deux-Sèvres', 'Lozère', 'Haute-Saône', 'Yonne', 'Morbihan',
        'Creuse', 'Maine-et-Loire', 'Indre', 'Eure-et-Loir', 'Dordogne', 'Loire-Atlantique',
        'Gers', 'Loir-et-Cher', 'Paris', 'Côtes-d\'Armor', 'Loire', 'Aveyron',
        'Puy-de-Dôme', 'Seine-et-Marne', 'Eure', 'Tarn', 'Meuse', 'Bouches-du-Rhône',
        'Loir-et-Cher', 'Charente-Maritime', 'Hautes-Alpes', 'Ardèche', 'Vienne',
        'Loiret', 'Ain', 'Côte-d\'Or', 'Haute-Loire', 'Loire-Atlantique', 'Nièvre',
        'Drôme', 'Corse-du-Sud', 'Bas-Rhin', 'Indre-et-Loire', 'Lot', 'Cantal',
        'Allier', 'Vosges', 'Savoie', 'Seine-Maritime', 'Vaucluse', 'Isère', 'Aube',
        'Tarn-et-Garonne', 'Gironde', 'Haute-Corse'
    ],
    "dep_code": [
        '53', '95', '52', '61', '80', '35', '93', '33', '30', '62', '81', '74', '34',
        '50', '83', '52', '16', '25', '92', '90', '02', '71', '19', '72', '59', '87',
        '94', '04', '64', '68', '66', '60', '62', '65', '47', '57', '85', '11', '91',
        '31', '29', '54', '55', '06', '14', '08', '79', '48', '70', '89', '56', '23',
        '49', '36', '28', '24', '44', '32', '41', '75', '22', '42', '12', '63', '77',
        '27', '81', '55', '13', '41', '17', '05', '07', '86', '45', '01', '21', '43',
        '44', '58', '26', '2A', '67', '37', '46', '15', '03', '88', '73', '76', '84',
        '38', '10', '82', '33', '2B'
    ]
}

# Creating a DataFrame
df = pd.DataFrame(data)

# Saving to a parquet file
output_path = "data/municipality_and_departments.parquet"
df.to_parquet(output_path, index=False)
