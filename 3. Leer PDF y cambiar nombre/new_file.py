import os
from PyPDF2 import PdfReader, PdfWriter
import datetime
from datetime import timedelta
import glob

# Get the current date
current_date = datetime.date.today()


directory = os.getcwd()
pdf_files = glob.glob(os.path.join(directory, '*.pdf'))

year = current_date.year
previous_month = current_date - timedelta(days=current_date.day)
month = previous_month.strftime("%m")
for pdf in pdf_files:
    try:
        # Open the PDF file
        with open(pdf, 'rb') as pdf_file:
            # Create a PdfReader object to read the PDF
            pdf_reader = PdfReader(pdf_file)
            # Get the first page of the PDF
            first_page = pdf_reader.pages[0]
            page_text = first_page.extract_text()
            # Search for the NEMOTECNICO of the instrument
            if "CFI" in page_text:
                start_char = "CFI"
            else:
                start_char = "CFM"
            # Get the index of the first letter of the nemotecnico
            start_index = page_text.index(start_char)
            # Get the whole string of the nemotecnico
            nemotecnico = page_text[start_index:].split()[0]
            if "CARACTERISTICAS" in nemotecnico:
                print("cambiar el nombre")
                nemotecnico = nemotecnico.replace("CARACTERISTICAS", "")
            name = str(year)+month+"-"+nemotecnico+".pdf"
        os.rename(pdf, name)        

    except:
        print("Error, no se pudo actualizar el nombre")