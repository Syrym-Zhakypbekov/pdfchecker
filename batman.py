import PyPDF2
import json
import os
from multiprocessing import Pool, cpu_count
import re

# Path to the PDF file
pdf_path = r'C:\Users\syrym\Downloads\admission2024\grant2024edge2.pdf'
output_path = r'C:\Users\syrym\Downloads\admission2024\list_words.json'

# Function to extract text from a range of pages in the PDF
def extract_text_from_pdf_pages(args):
    pdf_path, start_page, end_page = args
    page_texts = []
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        for page_num in range(start_page, end_page):
            page = reader.pages[page_num]
            text = page.extract_text()
            page_texts.append((page_num, text))
    return page_texts

# Function to find the phrases containing the word and their page numbers
def find_phrases_in_text(page_texts, word):
    phrases = []
    stop_pattern = re.compile(r'^(6B01|\d{2}\.\d{2}\.\d{2}|АТЫРАУСКАЯ ОБЛАСТЬ|B\d{3}|№ Фамилия, Имя, Отчество)')
    for page_num, text in page_texts:
        lines = text.split('\n')
        for i, line in enumerate(lines):
            if word in line:
                phrase = line.strip()
                j = i + 1
                while j < len(lines):
                    next_line = lines[j].strip()
                    if stop_pattern.match(next_line):
                        break
                    phrase += ' ' + next_line
                    j += 1
                phrases.append({"phrase": phrase, "page": page_num + 1})
    return phrases

# Function to process a range of pages
def process_pages(args):
    pdf_path, start_page, end_page = args
    page_texts = extract_text_from_pdf_pages((pdf_path, start_page, end_page))
    word_to_find = 'Список'
    phrases = find_phrases_in_text(page_texts, word_to_find)
    return phrases

def log_progress(current, total):
    print(f"Processing chunk {current + 1}/{total}")

if __name__ == '__main__':
    # Initialize the JSON file
    if not os.path.exists(output_path):
        with open(output_path, 'w', encoding='utf-8') as json_file:
            json.dump([], json_file, ensure_ascii=False, indent=4)

    # Get total number of pages
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        total_pages = len(reader.pages)

    # Number of processes
    num_processes = cpu_count()
    chunk_size = total_pages // num_processes
    ranges = [(pdf_path, i * chunk_size, (i + 1) * chunk_size if i < num_processes - 1 else total_pages) for i in range(num_processes)]

    # Process the PDF pages in parallel
    with Pool(num_processes) as pool:
        results = []
        for i, result in enumerate(pool.imap_unordered(process_pages, ranges)):
            results.append(result)
            log_progress(i, num_processes)

    # Flatten the list of results
    phrases_list = [phrase for sublist in results for phrase in sublist]

    # Add order to each phrase
    ordered_phrases = [{"order": idx + 1, **phrase} for idx, phrase in enumerate(phrases_list)]

    # Load the existing data from the JSON file
    with open(output_path, 'r', encoding='utf-8') as json_file:
        existing_data = json.load(json_file)

    # Update the data with the new phrases found
    existing_data.extend(ordered_phrases)

    # Save the updated data back to the JSON file
    with open(output_path, 'w', encoding='utf-8') as json_file:
        json.dump(existing_data, json_file, ensure_ascii=False, indent=4)

    print(f"All pages processed and phrases have been saved to {output_path}")
