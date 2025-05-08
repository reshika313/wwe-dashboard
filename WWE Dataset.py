import sqlite3
import pandas as pd

# Provide the path to your SQLite file
db_path = r'C:\Users\Reshika\OneDrive\Documents\VS Code\wwe_data\wwe_db.sqlite'  # Adjust as needed

# Connect to the SQLite database
conn = sqlite3.connect(db_path)

# 1. Get the structure of the 'cards' table
cards_structure = pd.read_sql("PRAGMA table_info(cards);", conn)
print("Structure of the 'cards' table:")
print(cards_structure)

# 2. Get the structure of the 'events' table (if you want to see it too)
events_structure = pd.read_sql("PRAGMA table_info(events);", conn)
print("\nStructure of the 'events' table:")
print(events_structure)

# 3. Get the structure of the 'promotions' table (if you want to see it too)
promotions_structure = pd.read_sql("PRAGMA table_info(promotions);", conn)
print("\nStructure of the 'promotions' table:")
print(promotions_structure)

# 4. Get the structure of the 'locations' table
locations_structure = pd.read_sql("PRAGMA table_info(locations);", conn)
print("\nStructure of the 'locations' table:")
print(locations_structure)

# 5. Get the structure of the 'matches' table (if you want to see it too)
matches_structure = pd.read_sql("PRAGMA table_info(matches);", conn)
print("\nStructure of the 'matches' table:")
print(matches_structure)

# 6. Get the structure of the 'match_types' table
match_types_structure = pd.read_sql("PRAGMA table_info(match_types);", conn)
print("\nStructure of the 'match_types' table:")
print(match_types_structure)

# 7. Get the structure of the 'wrestlers' table (if you want to see it too)
wrestlers_structure = pd.read_sql("PRAGMA table_info(wrestlers);", conn)
print("\nStructure of the 'wrestlers' table:")
print(wrestlers_structure)

# 8. Get the structure of the 'belts' table (to join with)
belts_structure = pd.read_sql("PRAGMA table_info(belts);", conn)
print("\nStructure of the 'belts' table:")
print(belts_structure)

# 9. Join 'cards', 'events', 'promotions', 'locations', 'matches', 'match_types', 'wrestlers', and 'belts' tables using the appropriate columns
query = """
SELECT cards.*, 
       events.name AS event_name, 
       promotions.name AS promotion_name,
       locations.name AS location_name,
       matches.*, 
       match_types.name AS match_type_name,
       winner.name AS winner_name,
       loser.name AS loser_name,
       belts.name AS belt_name  -- Adding belt name here
FROM cards
JOIN events ON cards.event_id = events.id
JOIN promotions ON cards.promotion_id = promotions.id
JOIN locations ON cards.location_id = locations.id
JOIN matches ON cards.id = matches.card_id
JOIN match_types ON matches.match_type_id = match_types.id
JOIN wrestlers AS winner ON matches.winner_id = winner.id
JOIN wrestlers AS loser ON matches.loser_id = loser.id
LEFT JOIN belts ON matches.title_id = belts.id;
"""

# Execute the query and get the full dataset (without any row limit)
cards_with_all_info_matches_types_and_wrestlers_belts = pd.read_sql(query, conn)

# 10. Remove the unwanted columns (info_html, match_html, and url)
cards_with_all_info_matches_types_and_wrestlers_belts_cleaned = cards_with_all_info_matches_types_and_wrestlers_belts.drop(columns=['info_html', 'match_html', 'url'], axis=1)

# 11. Remove all columns that have 'id' in their name
cards_final_cleaned = cards_with_all_info_matches_types_and_wrestlers_belts_cleaned.loc[:, ~cards_with_all_info_matches_types_and_wrestlers_belts_cleaned.columns.str.contains('id')]

# Display the cleaned data (all rows, after the join and column removal)
print("\nAll rows after joining 'cards', 'events', 'promotions', 'locations', 'matches', 'match_types', 'wrestlers' and 'belts' and removing unwanted columns:")
print(cards_final_cleaned)

# Show the structure of the final cleaned dataset
print("\nStructure of the final dataset:")
print(cards_final_cleaned.columns)

# 12. Save the final dataset to a CSV file
output_csv_path = r'C:\Users\Reshika\OneDrive\Documents\VS Code\wwe_data\wwe_full_dataset.csv'  # Adjust the output path as needed
cards_final_cleaned.to_csv(output_csv_path, index=False)

# Notify the user that the file has been saved
print(f"\nData has been saved to {output_csv_path}")

# Close the connection
conn.close()
