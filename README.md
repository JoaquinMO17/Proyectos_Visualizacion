# VisualizationU1

This project uses **PostgreSQL**, **Docker**, and **Visual Studio Code** to manage and analyze data.  
It includes a `docker-compose.yml` file to orchestrate the services and `content.json, users.csv, viewing_sessions.csv` files with the necessary data for the database.

---

## Step 1: Prerequisites

Before getting started, make sure you have installed:

- [Docker Desktop](https://www.docker.com/products/docker-desktop)  
  Download it from the official site according to your operating system (Windows, macOS, or Linux).  
  Once installed, open the Docker Desktop app to verify it’s running.

- [Visual Studio Code](https://code.visualstudio.com/) *(optional, recommended for editing and viewing the code)*  

---

### Installation & Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/JoaquinMO17/Proyectos_Visualizacion

## Step 2: Navigate into the project folder

    cd routetoyourrepositoryfolder/Proyectos_Visualizacion

## Step 3: Open Docker Desktop

Make sure Docker is running before continuing.

## Step 4: Download the Postgres image from Docker (if you don’t have it already)

    docker pull postgres

## Step 5: Start the containers

    docker compose up --build

This will download the required images, create the containers, and start them.

## Step 6: Check that everything is running

    docker ps

You should see two containers running:

- One for the application

- One for the PostgreSQL database

Copy the Container ID from the PostgreSQL container.

## Step 7: Run the etl.py script

    docker-compose run --rm web python -m scripts.etl

Replace <postgres_container_name> with the container ID you just copied.

## Project Data

content.json → Contains the dataset that will be loaded into the database videoanalysisdb.

docker-compose.yml → Defines the services (application and PostgreSQL database).

## Connecting to the Database

Find the container ID or name:

    docker ps


Execute the following command:

    docker exec -it <container_id> psql -U postgres -d videoanalysisdb


-U postgres → Default PostgreSQL user

-d videoanalysisdb → Name of the configured database

## Notes

All commands are executed from the terminal.

To stop the containers:

    docker compose down


If you edit the docker-compose.yml file, you’ll need to rebuild or restart the services.

## Troubleshooting

- Port 5432 already in use
PostgreSQL uses port 5432 by default. If you already have PostgreSQL running locally, Docker may fail to start the container.
**Solution**: Stop the local PostgreSQL service or change the port mapping in docker-compose.yml.

- docker-compose command not found
Newer Docker versions replaced docker-compose with docker compose.
**Solution**: Try running:

    docker compose up -d


- Containers start but exit immediately
This usually means there’s a configuration issue in docker-compose.yml or the database service crashed.
**Solution**: Check logs with:

    docker logs <container_id>


- Cannot connect to PostgreSQL inside the container

Ensure the container is running:

    docker ps


Double-check if you are using the correct container ID and database name.

- Permission denied when mounting files
If you are on Linux, you may need additional permissions for mounted volumes.
**Solution**: Run Docker with sudo or adjust file permissions.

## Authors

- Isaías De Jesús López Tzec

- Joaquin de Jesús Murguía Ortiz

- Valeria De Los Ángeles Paredes Dzib

- Damaris Esther Pech Aque

- Ana Paula Ramírez Romero

- Krishna Sandoval Cambranis

## GitHub Profiles:
@Iasisa

@JoaquinMO17

@valinyourarea

@damapech1

@AnaPauR

@Playmaker3334