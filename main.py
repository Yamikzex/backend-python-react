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

app = FastAPI()
tabula_path = os.path.join(os.path.dirname(__file__), "tabula", "tabula-1.0.5-jar-with-dependencies.jar")
tabula.environment_info()
# Configurar conexión a la base de datos SQL Server con autenticación de Windows
DATABASE_URL = "mssql+pyodbc://@DESKTOP-5NEKD7R\\SQLEXPRESS/pruebapractica?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"
engine = create_engine(DATABASE_URL)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Agrega la URL de tu aplicación de React
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.post("/uploadfile/")
async def upload_file(file: UploadFile = File(...)):
    try:
        # Leer el PDF y extraer las tablas
        tablas = tabula.read_pdf(file.file, pages='all', multiple_tables=True)
        
        # Crear un diccionario para almacenar los datos de las tablas
        data = {"tablas": []}
        
        for df in tablas:
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

        # Devolver el diccionario como respuesta JSON
        return JSONResponse(content={"jsonData": data, "databaseName": "pruebapractica"}, status_code=200)
    
    except Exception as e:
        return JSONResponse(content={"error": f"Error durante el proceso: {e}"}, status_code=500)

@app.get("/download-excel/")
async def download_excel():
    try:
        # Leer el archivo JSON generado anteriormente
        with open("tablas.json", "r") as json_file:
            data = json.load(json_file)
        
        # Convertir las tablas del JSON a un DataFrame de Pandas
        df_list = [pd.DataFrame(table) for table in data["tablas"]]
        
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

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)