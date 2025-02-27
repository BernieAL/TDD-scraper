first version
    locally built with all services running on local machine

second version
    dockerized, split all services into containers.
    all containers to be deployed on a host
    containers consist of 
        ui container
        nginx server (reverse proxy)
        db
        scrapers (1 for each source site)
        analysis + report gen
        emailing

third version
    split into frontend and backend
    
    frontend to be hosted using aws s3 + cloudfront
    
    backend to be set up with lambda functions acting as api

    lambda functions will spin up containers for scraping as needed etc