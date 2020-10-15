## Tests

```bash
# cd project-folder where located docker-compose.yml

# Options explanations:
# --build : Build images before starting containers.
# test : Service be run (defined in docker-compose.yml)
docker-compose up --build test
docker-compose down
```

Check logs:
```bash
docker-compose logs -f
```