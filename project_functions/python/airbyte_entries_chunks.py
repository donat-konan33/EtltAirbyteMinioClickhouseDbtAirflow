import json
import argparse


def chunk_france(chunk_size) -> None:
    """
    to chunk 99 departments of France with chunk_size
    """
    departments = "airbyte/france_departments.json"
    with open(departments, "r") as file:
        content = json.load(file)
        locations = content["departments"]["locations"]

        # Dividing the list into chunks of 10
        chunks = [locations[i:i + chunk_size] for i in range(0, len(locations), chunk_size)]

        # Displaying each chunk
    chunks_dict = {}
    for idx, chunk in enumerate(chunks):
        chunks_dict[f"Chunk {idx + 1}"] = f"{' | '.join(chunk)}"

    with open("airbyte/france_departments_chunks.json", "w", encoding="utf-8") as file:
        json.dump(chunks_dict, file, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk_size", help="To chunk a list of entries of departments with a given chunk_size number",
                        type=int)
    args = parser.parse_args()
    chunk_france(args.chunk_size)
