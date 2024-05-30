import json
import os
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse, JSONResponse
import tabula
import pandas as pd
from sqlalchemy import create_engine
from fastapi.middleware.cors import CORSMiddleware
import PyPDF2
from io import BytesIO
from fastapi import File, UploadFile
from sqlalchemy.exc import SQLAlchemyError
import pyodbc   
from fastapi.responses import JSONResponse, StreamingResponse
import uvicorn
import xml.etree.ElementTree as ET
import fitz
app = FastAPI()
tabula_path = os.path.join(os.path.dirname(__file__), "tabula", "tabula-1.0.5-jar-with-dependencies.jar")
tabula.environment_info()
# Configurar conexión a la base de datos SQL Server con autenticación de Windows
DATABASE_URL = "mssql+pyodbc://@DESKTOP-5NEKD7R\\SQLEXPRESS/pruebapractica?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"
engine = create_engine(DATABASE_URL)


table_data = None
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Agrega la URL de tu aplicación de React
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.post("/uploadfile/")
async def upload_file(file: UploadFile = File(...)):
    global table_data
    try:
        # Leer el PDF y extraer las tablas
        tablas = tabula.read_pdf(file.file, pages='all', multiple_tables=True)
        
        # Crear un diccionario para almacenar los datos de las tablas
        data = {"tablas": []}
        
        for df in tablas:
            # Llenar los valores NaN con una cadena vacía
            df.fillna('', inplace=True)
            
            # Convertir cada tabla a formato JSON y agregarla al diccionario
            data["tablas"].append(df.to_dict(orient="records"))

            # Guardar los datos en la base de datos
            try:
                headers = df.columns.tolist()
                df.columns = [f'Campo{j+1}' for j in range(len(df.columns))]
                df.loc[-1] = headers
                df.index = df.index + 1
                df.sort_index(inplace=True)
                df.to_sql('pdftable', con=engine, if_exists='append', index=False)
            except SQLAlchemyError as e:
                return JSONResponse(content={"error": f"Error al escribir en la base de datos: {e}"}, status_code=500)

        # Guardar los datos en la variable global
        table_data = data
        
        # Devolver el diccionario como respuesta JSON
        return JSONResponse(content={"jsonData": data, "databaseName": "pruebapractica"}, status_code=200)
    
    except Exception as e:
        return JSONResponse(content={"error": f"Error durante el proceso: {e}"}, status_code=500)

@app.get("/download-excel/")
async def download_excel():
    global table_data
    try:
        if table_data is None:
            return JSONResponse(content={"error": "No hay datos disponibles para descargar"}, status_code=400)
        
        # Convertir las tablas del JSON a un DataFrame de Pandas
        df_list = [pd.DataFrame(table) for table in table_data["tablas"]]
        
        # Guardar los DataFrames como hojas en un archivo Excel en memoria
        excel_bytes = BytesIO()
        with pd.ExcelWriter(excel_bytes) as writer:
            for i, df in enumerate(df_list):
                df.to_excel(writer, sheet_name=f"Tabla_{i+1}", index=False)
        
        # Devolver el archivo Excel como streaming response
        excel_bytes.seek(0)
        return StreamingResponse(content=excel_bytes, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": "attachment; filename=data.xlsx"})

    except Exception as e:
        return JSONResponse(content={"error": f"Error durante la conversión a Excel: {e}"}, status_code=500)
    
    
@app.post("/extract-text/")
async def extract_text(file: UploadFile = File(...)):
    try:
        pdf_content = await file.read()
        pdf_reader = PyPDF2.PdfReader(BytesIO(pdf_content))
        text = ""
        for page_number in range(len(pdf_reader.pages)):
            text += pdf_reader.pages[page_number].extract_text()
        return JSONResponse(content={"text": text}, status_code=200)
    except Exception as e:
        return JSONResponse(content={"error": f"Error durante el proceso: {e}"}, status_code=500)
    
@app.post("/extract-images/")
async def extract_images(file: UploadFile = File(...)):
  try:
    pdf_content = await file.read()
    images = []
    doc = fitz.open(stream=pdf_content, filetype="pdf")

    # Iterate over pages and extract images
    for page in doc:
      for img_index, img in enumerate(page.get_images(full=True), start=1):
        xref = img[0]
        base_image = doc.extract_image(xref)
        image_bytes = base_image["image"]

        # Convert image data to base64 string
        import base64
        encoded_image = base64.b64encode(image_bytes).decode("utf-8")

        # Append the base64 encoded image data to the list
        images.append(encoded_image)

    # Return the list of base64 encoded image data
    return JSONResponse(content={"images": images}, status_code=200)

  except Exception as e:
    return JSONResponse(content={"error": f"Error during the process: {e}"}, status_code=500)


    
@app.post("/uploadXML/")
async def upload_xml(file: UploadFile = File(...)):
    try:
        # Leer el contenido del archivo XML
        xml_content = await file.read()

        # Parsear el archivo XML
        root = ET.fromstring(xml_content)

        # Definir los namespaces necesarios
        namespaces = {
            'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
            'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
            'ext': 'urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2'
        }

        
        # Obtener los datos del documento
        documento_info = {}
        for tag in ['UBLVersionID', 'CustomizationID', 'ProfileID', 'ProfileExecutionID',
                    'ID', 'UUID', 'IssueDate', 'IssueTime']:
            element = root.find(f'.//cbc:{tag}', namespaces)
            if element is not None:
                documento_info[tag] = element.text
                # Obtener el número de factura

        numero_factura = root.find('.//cbc:ID', namespaces=namespaces).text
        documento_info['Numero Factura'] = numero_factura

        
        # Obtener los datos del emisor
        emisor = root.find('.//cac:SenderParty', namespaces)
        if emisor is not None:
            emisor_info = {}
            for tag in ['RegistrationName', 'CompanyID']:
                element = emisor.find(f'.//cbc:{tag}', namespaces)
                if element is not None:
                    emisor_info[tag] = element.text
            documento_info['Emisor'] = emisor_info

        # Obtener los datos del adquiriente
        adquiriente = root.find('.//cac:ReceiverParty', namespaces)
        if adquiriente is not None:
            adquiriente_info = {}
            for tag in ['RegistrationName', 'CompanyID']:
                element = adquiriente.find(f'.//cbc:{tag}', namespaces)
                if element is not None:
                    adquiriente_info[tag] = element.text
            documento_info['Adquiriente'] = adquiriente_info

        
        resultado_verificacion = root.find('.//cac:ResultOfVerification', namespaces)
        if resultado_verificacion is not None:
            resultado_verificacion_info = {}
            for tag in ['ValidatorID', 'ValidationResultCode', 'ValidationDate', 'ValidationTime']:
                element = resultado_verificacion.find(f'.//cbc:{tag}', namespaces)
                if element is not None:
                    resultado_verificacion_info[tag] = element.text
            documento_info['Resultado de verificacion'] = resultado_verificacion_info
            
        # Convertir los datos del documento a texto plano
        texto_plano = "\n**Datos del documento:**\n"
        for key, value in documento_info.items():
            if isinstance(value, dict):
                texto_plano += f"\n{key}:\n"
                for k, v in value.items():
                    texto_plano += f"  - {k}: {v}\n"
            else:
                texto_plano += f"- {key}: {value}\n"

        # Devolver el texto plano como respuesta
        return {"texto_plano": texto_plano}

    except Exception as e:
        return JSONResponse(content={"error": f"Error durante el proceso: {e}"}, status_code=500)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)