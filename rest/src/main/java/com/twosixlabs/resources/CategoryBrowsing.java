package com.twosixlabs.resources;

import com.twosixlabs.model.accumulo.ScanWrapper;
import com.twosixlabs.model.entities.MetadataValueStats;
import com.twosixlabs.model.entities.Project;
import com.twosixlabs.model.entities.ProjectMetadata;
import com.twosixlabs.model.entities.ProjectSearchResult;
import com.twosixlabs.muse_utils.App;
import com.twosixlabs.utils.QueryParser$;
import com.twosixlabs.view.Html;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.twosixlabs.model.accumulo.ScanWrapper.getSummaryTableCategoryCf;
import static com.twosixlabs.model.accumulo.ScanWrapper.getSummaryTableStatsCf;
import static com.twosixlabs.view.Html.projectListResultView;

@Path("muse")
public class CategoryBrowsing extends ResourceBase{
    @GET
    @Path("home")
    @Produces(MediaType.TEXT_HTML)
    public String serveWelcome() {
        StringBuilder rVal = new StringBuilder();
        try {
            // Generate multiple tables: Categories, Stats

            // Summary column family Categories
            HashMap<String, String> categoryData = new HashMap<>();

            HashMap<String, ConcurrentHashMap<String, Integer>> categories = getSummaryTableCategoryCf();
            categories.forEach((k,v) ->{
                categoryData.put(k, Integer.toString(v.size()));
            });
            rVal.append(Html.tableCategoriesWithCounts(getBaseUrl(),getBaseUrl()+"muse/categories/", "Categories", categoryData,false));

            // Summary column family Stats
            ArrayList<MetadataValueStats> statData = new ArrayList<>();
            HashMap<String, MetadataValueStats> statSummary = getSummaryTableStatsCf();
            statSummary.forEach((k,v) -> statData.add(v));
            String caption = new StringBuilder("Corpus Stats for ").append(ScanWrapper.getProjectCount())
                    .append(" Projects").toString();
            rVal.append(Html.welcomeStats(getBaseUrl(), caption, statData));
        } catch (Exception e1) {
            App.logException(e1);
        }finally {
            return rVal.toString();
        }
    }
    private QueryParser$ parseEng = QueryParser$.MODULE$.MODULE$;


    @GET
    @Path("search")
    @Produces(MediaType.TEXT_HTML)
    public String serveSearchResults(@QueryParam("q") String queryStringEncoded,
                                     @DefaultValue("25") @QueryParam("pageSize") int pageSize,
                                     @DefaultValue("1") @QueryParam("page") int page,
                                     @DefaultValue("stargazers_count") @QueryParam("metadataKey") String metadataKey,
                                     @DefaultValue("true") @QueryParam("cache") boolean cache,
                                     @QueryParam("projectRow") String projectRowEncoded) {

        String rVal = serveWelcome();

        try {
            if (projectRowEncoded != null){
                rVal = renderProjectFromRow(decode64(projectRowEncoded));
            } else if (queryStringEncoded != null) {
                long start = System.currentTimeMillis();
                String projectLink = null;
                projectLink = getAbsolutePath() + "?q=" + queryStringEncoded + "&page=" + page + "&projectRow=";
                String pageLink = getAbsolutePath() + "?q=" + queryStringEncoded + "&page=";
                String queryString = decode64(queryStringEncoded);
                List<List<String>> termsAndScans = parseEng.parse(queryString);
                if (termsAndScans.size() == 1 && termsAndScans.get(0).get(0).contains("Error")) {
                    throw new Exception(termsAndScans.get(0).get(0));
                } else {
                    HashMap<String, String> projectSet = ScanWrapper.query(termsAndScans, pageSize, page, metadataKey, cache, false);
                    if (projectSet.size() > 0){
                        long duration = System.currentTimeMillis() - start;
                        String caption = new StringBuilder(queryString).append(" (")
                                .append(duration).append(" msec)").toString();
                        rVal = projectListResultView(caption, projectSet, projectLink,
                                page, pageLink, getBaseUrl());
                    }
                }
            }
        } catch (UnsupportedEncodingException e) {
            App.logException(e);
        } catch (Exception e) {
            App.logException(e);
        }finally {
            return rVal;
        }
    }

    @GET
    @Path("search")
    @Produces(MediaType.APPLICATION_JSON)
    public ProjectSearchResult serveSearchResultsJson(@QueryParam("q") String queryStringEncoded,
                                                   @DefaultValue("25") @QueryParam("pageSize") int pageSize,
                                                   @DefaultValue("1") @QueryParam("page") int page,
                                                   @DefaultValue("stargazers_count") @QueryParam("metadataKey") String metadataKey,
                                                      @DefaultValue("true") @QueryParam("cache") boolean cache,
                                                      @DefaultValue("false") @QueryParam("countOnly") boolean countOnly,
                                                   @QueryParam("projectRow") String projectRow) {

        ProjectSearchResult rVal= null;

        try {
            if (queryStringEncoded != null) {
                List<List<String>> termsAndScans = parseEng.parse(new String(Base64.getDecoder().decode(queryStringEncoded)));
                if (termsAndScans.size() == 1 && termsAndScans.get(0).get(0).contains("Error")) {
                    throw new WebApplicationException(Response.status(400, termsAndScans.get(0).get(0)).build());
                } else {
                    long start = System.currentTimeMillis();
                    HashMap<String, String> projectSet;
                    if (countOnly){
                        projectSet = ScanWrapper.queryForCount(termsAndScans, pageSize, page, metadataKey, cache);
                    }else{
                        projectSet = ScanWrapper.query(termsAndScans, pageSize, page, metadataKey, cache, false);
                    }
                    long elapsedTime = System.currentTimeMillis() - start;
                    HashSet<Project> searchResultSet= new HashSet<>();

                    rVal = new ProjectSearchResult(new String(Base64.getDecoder().decode(queryStringEncoded)), elapsedTime, searchResultSet);
                    if (!countOnly) {
                        if (projectSet.size() > 0) {
                            // search is considered a category
                            //muse/categories/search/{categoryItem}/pages/{page}/projects/{projectRow}
                            String pLink = new StringBuilder(getBaseUrl()).append("muse/categories/search/")
                                    .append(queryStringEncoded).append("/pages/")
                                    .append(page).append("/projects/").toString();
                            projectSet.keySet().stream().forEach(r -> searchResultSet.add(new Project(pLink, r.toString())));
                        }
                    }
                    rVal.setCount(projectSet.size());
                }
            }else{
                throw new WebApplicationException(Response.status(400, "Empty Query String.").build());
            }
        } catch (WebApplicationException e) {
            App.logException(e);
            throw(e);
        }catch (Exception e) {
            App.logException(e);
            throw new WebApplicationException(Response.status(500, e.getMessage()).build());
        }

        return rVal;
    }

    // used to validate a search query
    @GET
    @Path("search/validate/{queryStringEncoded}")
    @Produces(MediaType.TEXT_HTML)
    public String serveValidation(@PathParam("queryStringEncoded") String queryStringBase64) throws UnsupportedEncodingException {
        String rVal = null;
        List<List<String>> termsAndScans = parseEng.parse(decode64(queryStringBase64));
        if (termsAndScans.size() ==1 && termsAndScans.get(0).get(0).contains("Error")) {
            throw new WebApplicationException(Response.status(400, termsAndScans.get(0).get(0)).build());
        }
        return rVal;
    }

    @GET
    @Path("query/{searchString}/pages/{page}/projects/{projectRow}")
    @Produces(MediaType.TEXT_HTML)
    public String serveSearchBasedProject(@PathParam("projectRow") String projectRow){
        return renderProjectFromRow(projectRow);
    }

    @GET
    @Path("help")
    @Produces(MediaType.TEXT_HTML)
    public String serveWelcomeFromHelp() {
        return serveWelcome();
    }

    @GET
    @Path("help/overview")
    @Produces(MediaType.TEXT_HTML)
    public String serveOverview() {
        return Html.overview();
    }

    @GET
    @Path("help/api")
    @Produces(MediaType.TEXT_HTML)
    public String serveApi() {
        ArrayList<String> apiCalls = new ArrayList<>();

        apiCalls.add(".../muse/search?q=&lt;base64-encoded boolean search expression&gt; || Custom search || text/html, json");
        apiCalls.add(" || Note: currently supports boolean expressions of exact match for row elements (version, name, date, repo, uuid, language, or topic) || ");
        apiCalls.add(new StringBuilder(".../muse/search?q=&lt;base64-encoded boolean search expression&gt; || A curl script example of a custom search (json):")
                .append("<br> #!/bin/bash")
                .append("<br># A curl example of search")
                .append("<br>q=$(echo -n \"language=java and language = c\" | base64)")
                .append("<br>out=$(curl -s -H \"Accept: application/json\" \"http://docker-accumulo:8052/muse/search?q=\"$q)")
                .append("echo $out | jq .")
                .append("||")
                .append("{")
                .append("  \"key\": \"\",")
                .append("  \"url\": null,")
                .append("  \"projectSet\": [")
                .append("    {")
                .append("      \"key\": \"\",")
                .append("      \"url\": \"http://docker-accumulo:8052/muse/categories/search/bGFuZ3VhZ2U9amF2YSBhbmQgbGFuZ3VhZ2UgPSBj/pages/1/projects/\",")
                .append("      \"miniMetadataUrl\": \"http://docker-accumulo:8052/muse/categories/search/bGFuZ3VhZ2U9amF2YSBhbmQgbGFuZ3VhZ2UgPSBj/pages/1/projects/NS4wfGRpYW1ldGVyfDIwMDktMDktMjZ8dWNpMjAxMHw3ZWJkZDk2Ni1mYmZiLTExZTQtOGM2Yy00ZjBkZTY3ZjUxMjA=\",")
                .append("      \"metadataUrl\": \"http://docker-accumulo:8052/muse/categories/search/bGFuZ3VhZ2U9amF2YSBhbmQgbGFuZ3VhZ2UgPSBj/pages/1/projects/NS4wfGRpYW1ldGVyfDIwMDktMDktMjZ8dWNpMjAxMHw3ZWJkZDk2Ni1mYmZiLTExZTQtOGM2Yy00ZjBkZTY3ZjUxMjA=/content/metadata\",")
                .append("      \"codeUrl\": \"http://docker-accumulo:8052/muse/categories/search/bGFuZ3VhZ2U9amF2YSBhbmQgbGFuZ3VhZ2UgPSBj/pages/1/projects/NS4wfGRpYW1ldGVyfDIwMDktMDktMjZ8dWNpMjAxMHw3ZWJkZDk2Ni1mYmZiLTExZTQtOGM2Yy00ZjBkZTY3ZjUxMjA=/content/code\"")
                .append("    },")
                .append("<br>...")
                .append("<br>,")
                .append("<br>    {")
                .append("<br>      \"key\": \"\",")
                .append("<br>      \"url\": \"http://docker-accumulo:8052/muse/categories/search/bGFuZ3VhZ2U9amF2YSBhbmQgbGFuZ3VhZ2UgPSBj/pages/1/projects/\",")
                .append("<br>      \"miniMetadataUrl\": \"http://docker-accumulo:8052/muse/categories/search/bGFuZ3VhZ2U9amF2YSBhbmQgbGFuZ3VhZ2UgPSBj/pages/1/projects/NS4wfGZvYW18MjAwOS0wMy0wNHx1Y2kyMDEwfDFiNjM5ZjU4LWZiZTgtMTFlNC1hNDFjLWEzNjZjMjAzZDlkNg==\",")
                .append("<br>      \"metadataUrl\": \"http://docker-accumulo:8052/muse/categories/search/bGFuZ3VhZ2U9amF2YSBhbmQgbGFuZ3VhZ2UgPSBj/pages/1/projects/NS4wfGZvYW18MjAwOS0wMy0wNHx1Y2kyMDEwfDFiNjM5ZjU4LWZiZTgtMTFlNC1hNDFjLWEzNjZjMjAzZDlkNg==/content/metadata\",")
                .append("<br>      \"codeUrl\": \"http://docker-accumulo:8052/muse/categories/search/bGFuZ3VhZ2U9amF2YSBhbmQgbGFuZ3VhZ2UgPSBj/pages/1/projects/NS4wfGZvYW18MjAwOS0wMy0wNHx1Y2kyMDEwfDFiNjM5ZjU4LWZiZTgtMTFlNC1hNDFjLWEzNjZjMjAzZDlkNg==/content/code\"")
                .append("<br>    }")
                .append("<br>  ],")
                .append("<br>  \"queryTimeSpanMsec\": 3759,")
                .append("<br>  \"queryString\": \"language=java and language = c\",")
                .append("<br>  \"count\": 25")
                .append("<br>}")
                .toString());

        apiCalls.add(".../muse/categories || list of available categories and their count (e.g., Languages, Topics, etc. || text/html, json");
        apiCalls.add(".../muse/categories || A curl example of obtaining available categories (json):<br> curl -i -H \"Accept: application/json\" \"http://docker-accumulo:8052/muse/categories\" || HTTP/1.1 200 OK<br>Content-Type: application/json <br> Content-Length: 169<br>{\"key\":\"Categories\",\"url\":\"http://docker-accumulo:8052/muse/categories\",\"mapValues\":{\"languages\":192,\"topics\":12,\"projectMetadataKeys\":576},\"arrayValues\":null,\"count\":3}");

        apiCalls.add(".../muse/categories/{category} || list of projects for a specific category (e.g., Java projects, etc. || text/html, json");
        apiCalls.add(".../muse/categories/{category} || A curl example (FIRST 25 projects):<br> curl -i -H \"Accept: application/json\" \"http://docker-accumulo:8052/muse/categories/topics/android\" || HTTP/1.1 200 OK<br>Content-Type: application/json<br>Transfer-Encoding: chunked\n[{\"key\":null,\"url\":\"http://docker-accumulo:8052/muse/categories/topics/android/pages/1/projects/\",\"miniMetadataUrl\":\"http://docker-accumulo:8052/muse/categories/topics/android/pages/1/projects/5.0%7Candroid-analyzer%7C2011-08-31%7Cuci2011%7C42d94faa-cda4-11e4-b452-53d4c71eef8b\",\"metadataUrl\":\"http://docker-accumulo:8052/muse/categories/topics/android/pages/1/projects/5.0%7Candroid-analyzer%7C2011-08-31%7Cuci2011%7C42d94faa-cda4-11e4-b452-53d4c71eef8b/content/metadata\",\"codeUrl\":\"http://docker-accumulo:8052/muse/categories/topics/android/pages/1/projects/5.0%7Candroid-analyzer%7C2011-08-31%7Cuci2011%7C42d94faa-cda4-11e4-b452-53d4c71eef8b/content/code\"},...,{...}] ");

        apiCalls.add(".../muse/categories/{category}/pages/{page} || Particular page list of project for a specific category (integer > 0) || text/html, json");
        apiCalls.add(".../muse/categories/{category}/pages/{page} || A curl example (lists 2ND set of 25 projects):<br> curl -i -H \"Accept: application/json\" \"http://docker-accumulo:8052/muse/categories/topics/android/pages/2\" || HTTP/1.1 200 OK<br>Content-Type: application/json<br>Transfer-Encoding: chunked<br>[{\"key\":null,\"url\":\"http://docker-accumulo:8052/muse/categories/topics/android/pages/2/projects/\",\"miniMetadataUrl\":\"http://docker-accumulo:8052/muse/categories/topics/android/pages/2/projects/5.0%7Candroid-binding%7C2011-08-31%7Cuci2011%7C40805bf8-cda5-11e4-9ded-1f42d6f68ca8\",\"metadataUrl\":\"http://docker-accumulo:8052/muse/categories/topics/android/pages/2/projects/5.0%7Candroid-binding%7C2011-08-31%7Cuci2011%7C40805bf8-cda5-11e4-9ded-1f42d6f68ca8/content/metadata\",\"codeUrl\":\"http://docker-accumulo:8052/muse/categories/topics/android/pages/2/projects/5.0%7Candroid-binding%7C2011-08-31%7Cuci2011%7C40805bf8-cda5-11e4-9ded-1f42d6f68ca8/content/code\"},...{...}]");

        apiCalls.add(".../muse/categories/{category}/pages/{page}/projects/{project} || Metadata listing for a particular project || text/html, json");
        apiCalls.add(".../muse/categories/{category}/pages/{page}/projects/{project} || A curl example (lists metadata stored in database):<br> curl -i -H \"Accept: application/json\" \"http://docker-accumulo:8052/muse/categories/topics/android/pages/2/projects/5.0%7Candroid-blueball%7C2012-11-01%7Cgoogle%7C8e49347e-da69-11e4-b44d-8f9a4cbb2bc7\" || HTTP/1.1 200 OK<br>Content-Type: application/json<br>Content-Length: 1515<br>{\"key\":null,\"url\":\"http://docker-accumulo:8052/muse/categories/topics/android/pages/2/projects/5.0%7Candroid-blueball%7C2012-11-01%7Cgoogle%7C8e49347e-da69-11e4-b44d-8f9a4cbb2bc7\",\"miniMetadataUrl\":\"http://docker-accumulo:8052/muse/categories/topics/android/pages/2/projects/5.0%7Candroid-blueball%7C2012-11-01%7Cgoogle%7C8e49347e-da69-11e4-b44d-8f9a4cbb2bc7\",\"metadataUrl\":\"http://docker-accumulo:8052/muse/categories/topics/android/pages/2/projects/5.0%7Candroid-blueball%7C2012-11-01%7Cgoogle%7C8e49347e-da69-11e4-b44d-8f9a4cbb2bc7/content/metadata\",\"codeUrl\":\"http://docker-accumulo:8052/muse/categories/topics/android/pages/2/projects/5.0%7Candroid-blueball%7C2012-11-01%7Cgoogle%7C8e49347e-da69-11e4-b44d-8f9a4cbb2bc7/content/code\",\"metadata\":{\"code\":\"./latest\",\"topics\":\"[networking, android, input-output]\",\"project_size\":\"639824\",\"repo\":\"google\",\"total_size\":\"818745\",\"created_at\":\"2012-11-01T12:12:12Z\",\"description\":\"Ummm... it&#39;s a blue ball\",\"uuid\":\"8e49347e-da69-11e4-b44d-8f9a4cbb2bc7\",\"corpus_release\":\"2.0\",\"site\":\"google\",\"full_name\":\"android-blueball\",\"crawled_date\":\"2012-11-01T12:12:12Z\",\"html_url\":\"https://code.google.com/archive/p/android-blueball\",\"site_specific_id\":\"10957\",\"crawler_metadata\":\"[\\\".\\\\/google\\\\/info.json\\\",\\\".\\\\/google\\\\/languages.json\\\",\\\".\\\\/google\\\\/totalSize.json\\\",\\\".\\\\/google\\\\/commits.json\\\",\\\".\\\\/google\\\\/sloc.json\\\",\\\".\\\\/google\\\\/topics.json\\\"]\",\"name\":\"android-blueball\",\"metadata_size\":\"178921\",\"id\":\"10957\",\"languageMain\":\"Java\",\"timestamp\":\"1460922142\"}} | ");

        apiCalls.add(".../muse/metadata/{key}/pagesize/{pageSize}/page/{page} || Metadata values for key (Use of pagesize/page: if looking for 1000 values, set pagesize=1000 and page=1) || json");
        apiCalls.add(".../muse/metadata/{key}/pagesize/{pageSize}/page/{page} || A curl example (lists the 1st set of 10 fork amounts):<br> curl -i -H \"Accept: application/json\" \"http://docker-accumulo:8052/muse/metadata/forks/pagesize/10/page/1\" || HTTP/1.1 200 OK <br>Content-Type: application/json<br> Content-Length: 152 <br> {\"key\":\"forks\",\"url\":\"http://docker-accumulo:8052/muse/metadata/forks/pagesize/10/page/1\",\"values\":[\"1\",\"1\",\"9\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\"],\"count\":10}");

        apiCalls.add(".../muse/metadata/{key}/pagesize/{pageSize}/page/{page}/unique || Unique metadata values for key (Use of pagesize/page: if looking for 1000 values, set pagesize=1000 and page=1) || json");
        apiCalls.add(".../muse/metadata/{key}/pagesize/{pageSize}/page/{page}/unique || A curl example (lists the 1st set of 10 fork amounts):<br> curl -i -H \"Accept: application/json\" \"http://docker-accumulo:8052/muse/metadata/forks/pagesize/10/page/1/unique\" || HTTP/1.1 200 OK <br> Content-Type: application/json <br> Content-Length: 130 <br> {\"key\":\"forks\",\"url\":\"http://docker-accumulo:8052/muse/metadata/forks/pagesize/10/page/1/unique\",\"values\":[\"0\",\"1\",\"9\"],\"count\":3}");

        apiCalls.add(".../muse/stats/count/metadata/{key}/pagesize/{pageSize}/page/{page} || Count of unique metadata items || json");
        apiCalls.add(".../muse/stats/count/metadata/{key}/pagesize/{pageSize}/page/{page} || A curl example of counting the number of projects that list a value for the key \"languageMain:\" curl -i -H \"Accept: application/json\" \"http://docker-accumulo:8052/muse/stats/count/metadata/languageMain/pagesize/500000/page/1\" || HTTP/1.1 200 OK<br>Content-Type: application/json<br>Content-Length: 150<br>{\"key\":\"languageMain\",\"url\":\"http://docker-accumulo:8052/muse/stats/count/metadata/languageMain/pagesize/500000/page/1\",\"values\":[\"432001\"],\"count\":1}");

        apiCalls.add(".../muse/stats/ave/metadata/{key}/pagesize/{pageSize}/page/{page} || Average value of metadata || json");
        apiCalls.add(".../muse/stats/ave/metadata/{key}/pagesize/{pageSize}/page/{page} || A curl example to get the average project size of the 1st 100000 projects:<br> curl -i -H \"Accept: application/json\" \"http://docker-accumulo:8052/muse/stats/ave/metadata/project_size/pagesize/100000/page/1\" || HTTP/1.1 200 OK<br>Content-Type: application/json<br>Content-Length: 158<br>{\"key\":\"project_size\",\"url\":\"http://docker-accumulo:8052/muse/stats/ave/metadata/project_size/pagesize/100000/page/1\",\"values\":[\"3.891199208581E7\"],\"count\":1}");

        apiCalls.add(".../muse/stats/minmax/metadata/{key}/pagesize/{pageSize}/page/{page} || Minimum and maximum (as a vector) of the projoct metadata key value || json");
        apiCalls.add(".../muse/stats/minmax/metadata/{key}/pagesize/{pageSize}/page/{page} || A curl example to get the minimum and maximum number of watchers for the first 5000 projects:<br>curl -i -H \"Accept: application/json\" \"http://docker-accumulo:8052/muse/stats/minmax/metadata/watchers/pagesize/5000/page/1\" || HTTP/1.1 200 OK<br>Content-Type: application/json<br>Content-Length: 148<br>{\"key\":\"watchers\",\"url\":\"http://docker-accumulo:8052/muse/stats/minmax/metadata/watchers/pagesize/5000/page/1\",\"values\":[\"0.0\",\"11895.0\"],\"count\":2}");

        apiCalls.add(".../muse/categories/{category}/{categoryItem}/pages/{page}/projects/{projectRow}/content/{type: 'code' or 'metadata'} || Get the link to binary (code or metadata) content || json");
        apiCalls.add(".../muse/categories/{category}/{categoryItem}/pages/{page}/projects/{projectRow}/content/{type: 'code' or 'metadata'} || A curl example getting the file link to the code tgz for a particular project: <br>curl -i -H \"Accept: application/json\" \"http://docker-accumulo:8052/muse/categories/topics/android/pages/1/projects/5.0%7Candroid-15-wmp%7C2012-11-01%7Cgoogle%7C306b11fc-da68-11e4-bb42-af79f95f41cb/content/code\" || HTTP/1.1 200 OK<br>Content-Type: application/json<br>Content-Length: 888<br>{\"key\":\"codeFilePath\",\"url\":\"http://docker-accumulo:8052/muse/categories/topics/android/pages/1/projects/5.0%7Candroid-15-wmp%7C2012-11-01%7Cgoogle%7C306b11fc-da68-11e4-bb42-af79f95f41cb/content/code\",\"miniMetadataUrl\":\"http://docker-accumulo:8052/muse/categories/topics/android/pages/1/projects/5.0%7Candroid-15-wmp%7C2012-11-01%7Cgoogle%7C306b11fc-da68-11e4-bb42-af79f95f41cb\",\"metadataUrl\":\"http://docker-accumulo:8052/muse/categories/topics/android/pages/1/projects/5.0%7Candroid-15-wmp%7C2012-11-01%7Cgoogle%7C306b11fc-da68-11e4-bb42-af79f95f41cb/content/metadata\",\"codeUrl\":\"http://docker-accumulo:8052/muse/categories/topics/android/pages/1/projects/5.0%7Candroid-15-wmp%7C2012-11-01%7Cgoogle%7C306b11fc-da68-11e4-bb42-af79f95f41cb/content/code\",\"metadata\":{\"codeFilePath\":\"/data/3/0/6/b/1/1/f/c/306b11fc-da68-11e4-bb42-af79f95f41cb/306b11fc-da68-11e4-bb42-af79f95f41cb_code.tgz\"}}");

        return Html.api(getBaseUrl(), apiCalls);
    }

    @GET
    @Path("settings")
    @Produces(MediaType.TEXT_HTML)
    public String settings(){
        return serveWelcome();
    }

    @GET
    @Path("projects/{projectRow}")
    @Produces(MediaType.TEXT_HTML)
    public String serveProjectFromSearch(@PathParam("projectRow") String projectRow){
        return renderProjectFromRow(projectRow);
    }

    @GET
    @Path("projects/{projectRow}")
    @Produces(MediaType.APPLICATION_JSON)
    public ProjectMetadata serveProjectFromSearchJson(@PathParam("projectRow") String projectRow){
        return getProjectMetadataEntity(projectRow);
    }

    @GET
    @Path("projects/{projectRow}/metadata/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    public ProjectMetadata serveCategoryBasedProjectMetadataJson(@PathParam("categoryItem") String categoryItemBase64,
                                                                 @PathParam("page") Integer page,
                                                                 @PathParam("projectRow") String projectRowBase64,
                                                                 @PathParam("key") String keyBase64) {
        return getProjectMetadataValue(decode64(projectRowBase64),decode64(keyBase64));
    }



}

