
[![Licencia](https://img.shields.io/badge/Licencia-MIT-blue.svg)](LICENSE)

# Partidos Azure App

Bienvenido a mi aplicación de partidos, una plataforma interactiva diseñada para poder seguir en directo los partidos de futbol y sus resultados.

## Tabla de Contenidos
- [Funcionalidades Principales](#funcionalidades-principales)
- [Diagrama del proyecto](#diagrama-del-proyecto)
- [Instrucciones de Uso](#instrucciones-de-uso)
  - [Prerequisitos](#prerequisitos)
  - [Instalación](#instalación)
  - [Tests](#tests)
- [Tecnologías Utilizadas](#tecnologias-utilizadas)
- [Licencia](#licencia)
- [Contacto](#contacto)

## Funcionalidades Principales

- **Obtencion de las ligas:** Permite obtener las ligas de futbol de manera dinamica.

- **Obtencion de los equipos:** Permite obtener los equipos de las ligas de futbol de manera dinamica junto con sus escudos y almacenarlos en una cuenta de almacenamiento.

- **Obtencion de los partidos:** Permite obtener los partidos de futbol de manera dinamica y actualizada con sus resultados.

- **Visualizar los partidos:** Permite navegar a traves de los dias para visualizar los diferentes partidos disputados.

- **Detalle de los partidos:** Permite obtener el detalle de los partidos que se han disputado con informacion relevante.


## Diagrama del proyecto

![Diagrama](./diagrama/diagrama.png)

## Instrucciones de Uso

### Prerequisitos

1. Antes de comenzar, asegúrate de tener instalado Docker en tu máquina. Puedes descargarlo [aquí](https://www.docker.com/get-started).

2. Crea una cuenta o utiliza una existente de Microsoft Azure. Puedes crear la cuenta [aquí](https://azure.microsoft.com/en-us/free/open-source).

3. Una vez creada la cuenta de Azure, deberas crear una cuenta de almacenamiento. Esta cuenta de almacenamiento debe ser un ADLS Gen2 donde se almacenaran los datos de los escudos de los equipos en el Cloud.

4. Agrega las credenciales de la cuenta de almacenamiento para los DAGS en `dags/python/src/datalake/confconexiondatalake.py` 

5. También agrega las credenciales de la cuenta de almacenamiento para la aplicacion web en `web/src/config.py`

### Instalación

Para ejecutar la aplicación con Docker:

1. Clona este repositorio con el siguiente comando:

    ```bash
    git clone https://github.com/nachodorado98/Flask-Partidos-Azure.git
    ```

2. Navega al directorio del proyecto.

3. Ejecuta el siguiente comando para construir y levantar los contenedores:

    ```bash
    docker-compose up -d
    ```

4. Modifica, si crees necesario, los paises y ligas que se van a obtener en el DAG Equipos accediendo a `dags/python/src/config.py`

5. Inicia el DAG Equipos en la interfaz de Apache Airflow para obtener y almacenar los datos de las ligas y los equipos: `http://localhost:8080`.

6. Modifica, si crees necesario, la fecha inicial de los partidos que se van a obtener dentro de la funcion `fechas_etl()` en el DAG Partidos accediendo a `dags/dag_partidos.py`

4. Una vez el DAG Equipos ha finalizado, inicia el DAG Partidos en la interfaz de Apache Airflow para obtener y almacenar los datos de los partidos: `http://localhost:8080`.

### Tests

Para ejecutar los tests de las ETL y la aplicacion:

1. Asegúrate de que los contenedores estén en funcionamiento. Si aún no has iniciado los contenedores, utiliza el siguiente comando:

    ```bash
    docker-compose up -d
    ```

2. Dentro del contenedor del servicio `scheduler`, cambia al directorio de los tests:

    ```bash
    cd dags/python/tests
    ```

3. Ejecuta el siguiente comando para ejecutar los tests utilizando pytest:

    ```bash
    pytest
    ```

Este comando ejecutará todas las pruebas en el directorio `tests` y mostrará los resultados en la consola.

4. Dentro del contenedor del servicio `web`, cambia al directorio de los tests:

    ```bash
    cd tests
    ```

5. Ejecuta el siguiente comando para ejecutar los tests utilizando pytest:

    ```bash
    pytest
    ```

Este comando ejecutará todas las pruebas en el directorio `tests` y mostrará los resultados en la consola.


## Tecnologías Utilizadas

- [![python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)](https://www.python.org/)
- [![airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
- [![azure](https://img.shields.io/badge/microsoft%20azure-0089D6?style=for-the-badge&logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com/en-us/free/open-source)
- [![postgres](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
- [![docker](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
- [![flask](https://img.shields.io/badge/Flask-000000?style=for-the-badge&logo=flask&logoColor=white)](https://flask.palletsprojects.com/)
- [![HTML](https://img.shields.io/badge/HTML5-E34F26?style=for-the-badge&logo=html5&logoColor=white)](https://developer.mozilla.org/en-US/docs/Web/HTML)
- [![css](https://img.shields.io/badge/CSS3-1572B6?style=for-the-badge&logo=css3&logoColor=white)](https://developer.mozilla.org/en-US/docs/Web/CSS)
- [![JavaScript](https://img.shields.io/badge/JavaScript-323330?style=for-the-badge&logo=javascript&logoColor=F7DF1E)](https://developer.mozilla.org/en-US/docs/Web/JavaScript)

## Licencia

Este proyecto está bajo la licencia MIT. Para mas informacion ver `LICENSE.txt`.
## 🔗 Contacto
[![portfolio](https://img.shields.io/badge/proyecto-000?style=for-the-badge&logo=ko-fi&logoColor=white)](https://github.com/nachodorado98/Flask-Partidos-Azure.git)

[![email](https://img.shields.io/badge/Gmail-D14836?style=for-the-badge&logo=gmail&logoColor=white)](mailto:natxo98@gmail.com)

[![linkedin](https://img.shields.io/badge/linkedin-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/nacho-dorado-ruiz-339209237/)
