import os
import json
import argparse

def main():
    large_dir = "/home/ned/data/scanner_calls/logs/output/large_model_faster"
    medium_dir = "/home/ned/data/scanner_calls/logs/output/medium_model_faster"
    output_file = "transcript_comparison.json"

    results = []

    if not os.path.exists(large_dir) or not os.path.exists(medium_dir):
        print("One or both directories do not exist.")
        return

    for file_name in os.listdir(large_dir):
        if not file_name.endswith('.json'):
            continue
        
        large_path = os.path.join(large_dir, file_name)
        medium_path = os.path.join(medium_dir, file_name)
        
        # Check if the file exists in the medium directory as well
        if os.path.exists(medium_path):
            try:
                with open(large_path, 'r') as f:
                    large_data = json.load(f)
                with open(medium_path, 'r') as f:
                    medium_data = json.load(f)
                
                wav_filename = large_data.get("filename", file_name)
                
                results.append({
                    "filename": wav_filename,
                    "large_transcript": large_data.get("transcript", ""),
                    "medium_transcript": medium_data.get("transcript", "")
                })
            except Exception as e:
                print(f"Error reading {file_name}: {e}")

    with open(output_file, 'w') as f:
        json.dump(results, f, indent=4)
        
    print(f"Comparison complete. Found {len(results)} matching records. Saved to {output_file}")

if __name__ == "__main__":
    main()
