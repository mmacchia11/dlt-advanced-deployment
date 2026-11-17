
def lambda_handler(event, context):
    import os
    from pathlib import Path

    import dlt
    from dlt.sources.helpers import requests

    BASE_URL = "https://api.github.com/repos/dlt-hub/dlt/issues"

    # 1. Obtener las páginas de la API de GitHub
    def pagination(url):
        while True:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            yield response.json()

            if "next" not in response.links:
                break

            url = response.links["next"]["url"]

    # 2. Definir el recurso incremental de dlt
    @dlt.resource(
        table_name="issues",
        write_disposition="merge",
        primary_key="id",
    )
    def get_issues(
        updated_at=dlt.sources.incremental(
            "updated_at",
            initial_value="1970-01-01T00:00:00Z",
        )
    ):
        url = (
            f"{BASE_URL}?since={updated_at.last_value}"
            "&per_page=100"
            "&sort=updated"
            "&direction=desc"
            "&state=open"
        )

        yield from pagination(url)

    # 3. Configurar las rutas según el entorno
    is_lambda = bool(os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))

    base_dir = (
        Path("/tmp")
        if is_lambda
        else Path(os.environ.get("TEMP", os.environ.get("TMP", ".")))
        / "dlt_lambda_lab"
    )

    base_dir.mkdir(parents=True, exist_ok=True)

    # 4. Crear el pipeline de dlt
    pipeline = dlt.pipeline(
        pipeline_name="github_issues_lambda",
        destination=dlt.destinations.duckdb(
            str(base_dir / "github_issues_lambda.duckdb")
        ),
        dataset_name="github_data_merge",
        pipelines_dir=str(base_dir / ".dlt_pipelines"),
    )

    # 5. Ejecutar Extract, Normalize y Load
    load_info = pipeline.run(get_issues())

    print(pipeline.last_trace.last_normalize_info)
    print(load_info)

    # 6. Validar el resultado
    if load_info.has_failed_jobs:
        raise RuntimeError("El pipeline terminó con cargas fallidas")

    if load_info.loads_ids:
        print(f"Cargas completadas: {len(load_info.loads_ids)}")
    else:
        print("No se generaron cargas nuevas")

    return {
        "status": "success",
        "pipeline": pipeline.pipeline_name,
        "loads": len(load_info.loads_ids),
    }
