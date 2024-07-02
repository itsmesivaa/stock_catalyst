# StockCatalyst: Automated Stock Analysis Platform
`StockCatalyst web application designed to empower you, the modern investor, with the time-tested stock trading methodology used by market wizards like Stan Weinstein, Mark Minervini, and William O'Neil.  These legendary figures have built their success on proven strategies, and now, StockCatalyst puts that same power in your hands.Imagine automating tedious stock analysis, freeing yourself to focus on making informed investment decisions.  StockCatalyst does the heavy lifting, leveraging these market-proven strategies to identify high-growth potential stocks with serious upside.  No longer do you need to spend countless hours sifting through data – StockCatalyst streamlines the process, allowing you to focus on what matters most: building your wealth.`

`This app goes beyond basic stock analysis.  By incorporating the wisdom of market wizards, StockCatalyst equips you with a powerful edge.  Gain insights you might have missed, discover hidden gems with explosive potential, and make smarter investment decisions – all with the help of StockCatalyst.`
# Key Contributions:

  * Developed a robust web scraping tool leveraging Python to extract real-time stock prices from Yahoo Finance and fundamental evaluation metrics from MarketSmith websites.
  * Implemented time-tested investment strategies including Stan Weinstein's stage analysis, Mark Minervini's trend template, and elements of the CANSLIM approach by William O'Neil.
  * Designed and built a user-friendly interface using Streamlit, enabling intuitive display and interaction with analysis results for users.
  * Automated data collection and analysis tasks, significantly improving reliability and reducing manual effort in stock screening and evaluation.
# Outcome:

  StockCatalyst successfully integrated advanced web scraping techniques with sophisticated investment strategies, providing users with actionable insights into high-strength stocks. 
  The project not only demonstrated technical proficiency in web scraping and data analysis but also showcased the ability to translate 
  complex investment methodologies into practical, automated solutions.
# 🚀Future Roadmap
Some potential features for future releases:

  * More advanced forecasting models like LSTM
  * Scalable to more trading strategies
  * Portfolio optimization and tracking
  * Additional fundamental data & User account system

# Below were the steps to setup for Backend workflow EC2 instance 

# 1. Login with your AWS console and launch an EC2 instance

Run the following commands basic commands

    sudo apt-get update -y
    
    sudo apt-get upgrade

# Install Docker

    curl -fsSL https://get.docker.com -o get-docker.sh
    
    sudo sh get-docker.sh
    
    sudo usermod -aG docker ubuntu
    
    newgrp docker

# 3.Cloning Github repository project to EC2 instance

To Cloning private git repository need to generate token on Github 

    git clone https://<generatedtoken>@github.com/<your account or organization>/<repo>.git
    
# 4.Building Docker images 
Building Docker file on EC2 instance and moving images into Docker hub (For Backend workflow)

    docker build -f Dockerfile_webscrape -t itsmesivaa/webscrape:latest .

Building Docker file on EC2 instance and moving images into Docker hub (For Frontend workflow)

    docker build -f Dockerfile_stockcatalyst -t itsmesivaa/stockcatalyst:latest .

Checking listed images after successful build

    docker images -a  

Running docker images on EC2 instance all the time even after closing
Note: Running docker with -d will indefinetely run the container even after the instance close i.e, 24hours running on EC2 instance

# Backend docker workflow:

    docker run itsmesivaa/webscrape

# Frontend docker workflow:

    docker run -p 8501:8501 itsmesivaa/stockcatalyst

# To lists the containers running on your Docker host.

    docker ps

#Command to stop docker container

    docker stop container_id

#Deleting Docker containers

    docker rm $(docker ps -a -q)


# 5. Connect DockerHub

    docker login

# Moving built docker image to DockerHub
Backend docker file:

    docker push itsmesivaa/webscrape:latest

Fronend docker file:

    docker push itsmesivaa/stockcatalyst:latest

# Remove Docker images
Backend docker file:

    docker rmi itsmesivaa/webscrape:latest

Fronend docker file:

    docker rmi itsmesivaa/stockcatalyst:latest

# Pulling images from DockerHub

Backend docker file:

    docker pull itsmesivaa/webscrape

Fronend docker file:

    docker pull itsmesivaa/stockcatalyst