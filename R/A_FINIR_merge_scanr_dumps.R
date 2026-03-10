merge_scanr_dumps <- function(
  main_folder = "C:/Users/claverdet/Documents/data/scanr/" 
){

  files_to_import = data.frame(path = file.path(main_folder, "organizations_denormalized.parquet" ) )
  #et de la on peut ajouter nos noms "denormalized.parquet" et reconstituer les url des 4 fichiers .parquet de scanr
  
# notre but est de construire une table vide "orga et leurs publi" et de la remplir
req_orga_et_publi <- sprintf("CREATE TABLE org_publications AS
SELECT
  p.id AS publication_id,
  org.id AS organization_id
FROM '%s' p,
UNNEST(p.organizations) org", path_organization)

con <- establish_connection_with_DBI()

test <- DBI::dbGetQuery(con, req_orga_et_publi)

req_orga_et_projet <- "SELECT
  pr.id AS project_id,
  part.organization.id AS organization_id
FROM 'projects.parquet' pr,
UNNEST(pr.participants) part"

req_orga_et_brevet <- "SELECT
  pt.id,
  app.organization.id
FROM 'patents.parquet' pt,
UNNEST(pt.applicants) app"

requete_orga_et_publi <- "SELECT
  p.publication_id,
  o.id,
  o.label.default AS organization_name
FROM (

  SELECT
    p.id AS publication_id,
    org.id AS organization_id
  FROM 'publications.parquet' p,
  UNNEST(p.organizations) org

) p

JOIN 'organizations.parquet' o
ON p.organization_id = o.id"

requete_orga_et_publi
}