package com.twosixlabs.resources;

import com.twosixlabs.model.accumulo.ScanWrapper;
import com.twosixlabs.model.entities.CorpusMetadata;
import com.twosixlabs.model.entities.Project;
import com.twosixlabs.model.entities.ProjectMetadata;
import com.twosixlabs.muse_utils.App;
import com.twosixlabs.view.Html;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.twosixlabs.view.Html.listView;

@Path("muse/categories")
public class CategoryItem extends ResourceBase {

    private static final ConcurrentHashMap<String, Integer> CATEGORY_ELEMENTS = new ConcurrentHashMap<>();

    private static ConcurrentHashMap<String, Integer> getCategoryMap(){
        ConcurrentHashMap<String, Integer> rVal = CATEGORY_ELEMENTS;
        if (CATEGORY_ELEMENTS.size() == 0){
            CATEGORY_ELEMENTS.put("languages", getSpecificCategory("languages").size());
            CATEGORY_ELEMENTS.put("topics", getSpecificCategory("topics").size());
            CATEGORY_ELEMENTS.put("projectMetadataKeys", getSpecificCategory("projectMetadataKeys").size());
        }

        return rVal;
    }
    @GET
    @Produces(MediaType.TEXT_HTML)
    public String listElements() {
        return (listView("Categories",getCategoryMap(), getAbsolutePath(),getBaseUrl(), false));
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public CorpusMetadata listElementsJson() {
        CorpusMetadata cm = new CorpusMetadata();
        cm.setMapValues(getCategoryMap());
        return (CorpusMetadata)setupMetadataValues(cm,"Categories");
    }

    @GET
    @Path("{category}")
    @Produces(MediaType.TEXT_HTML)
    public String getCategory(@PathParam("category") String category) {
        ConcurrentHashMap<String, Integer> categoryItems = getSpecificCategory(category);
        HashMap<String, String> itemSet = new HashMap<>();
        categoryItems.forEach((item, count) -> itemSet.put(item,String.valueOf(count)));
        return Html.tableCategoriesWithCounts(getBaseUrl(),getAbsolutePath(), category.toUpperCase(), itemSet, true);
    }

    @GET
    @Path("{category}")
    @Produces(MediaType.APPLICATION_JSON)
    public CorpusMetadata getCategoryJson(@PathParam("category") String category) {
        ConcurrentHashMap<String, Integer> categoryItems = getSpecificCategory(category);

        return setupMetadataValues(category, categoryItems);
    }

    // serve up projects based on language or topic
    @GET
    @Path("{category}/{categoryItem: .+|.+\\/pages|.+\\/}")
    @Produces(MediaType.TEXT_HTML)
    public String serveCategoryBasedProjects(@PathParam("category") String category, @PathParam("categoryItem") String categoryItem) {
        Boolean pagesStrAdded = categoryItem.contains("/pages");
        categoryItem = pagesStrAdded ? categoryItem.substring(0, categoryItem.lastIndexOf("/pages")) : categoryItem;
        return RenderCategoryItemBasedProjects(category.replaceAll(".$", ""), categoryItem, categoryItem, 1);
    }

    // serve up projects based on categoryItem
    @GET
    @Path("{category}/{categoryItem: .+|.+\\/pages|.+\\/}")
    @Produces(MediaType.APPLICATION_JSON)
    public HashSet<Project> serveCategoryBasedProjectsJson(@PathParam("category") String category,@PathParam("categoryItem") String categoryItemBase64) {

        Boolean pagesStrAdded = categoryItemBase64.contains("/pages");
        String base64String = pagesStrAdded ? categoryItemBase64.substring(0, categoryItemBase64.lastIndexOf("/pages")) : categoryItemBase64;
        String categoryItem = new String(Base64.getDecoder().decode(base64String));
        return fetchCategoryItemBasedProject(category.replaceAll(".$", ""),categoryItem, categoryItem, 1);
    }

    @GET
    @Path("{category}/{categoryItem}/pages/{page: \\d+|\\d+\\/projects}")
    @Produces(MediaType.TEXT_HTML)
    public String serveCategoryBasedProjectsPerPage(@PathParam("category") String category,
                                                    @PathParam("categoryItem") String categoryItem,
                                             @PathParam("page") String page) {
        int pageVal = page.contains("/projects")? Integer.parseInt(page.substring(0,page.lastIndexOf("/projects")))
                : Integer.parseInt(page);
        return RenderCategoryItemBasedProjects(category.replaceAll(".$", ""),categoryItem, categoryItem, pageVal);
    }

    // ths call is a deadend for links from search
    // but perhaps categoryItem can be used as a search
    // search string
    @GET
    @Path("{category}/{categoryItem}/pages/{page: \\d+|\\d+\\/projects}")
    @Produces(MediaType.APPLICATION_JSON)
    public HashSet<Project> serveCategoryBasedProjectsPerPageJson(@PathParam("category") String category,
                                                                  @PathParam("categoryItem") String categoryItemBase64,
                                                           @PathParam("page") String page) {

        int pageVal = page.contains("/projects")? Integer.parseInt(page.substring(0,page.lastIndexOf("/projects")))
                : Integer.parseInt(page);
        String categoryItem = decode64(categoryItemBase64);
        return fetchCategoryItemBasedProject(category.replaceAll(".$", ""),categoryItem,categoryItem, pageVal);
    }

    @GET
    @Path("{category}/{categoryItem}/pages/{page}/projects/{projectRow}")
    @Produces(MediaType.TEXT_HTML)
    public String serveCategoryBasedProject(@PathParam("categoryItem") String categoryItem,
                                             @PathParam("page") Integer page,
                                             @PathParam("projectRow") String projectRowBase64) {

        String rVal = null;
        try {
            rVal =  renderProjectFromRow(decode64(projectRowBase64));
        } catch (Exception e) {
            App.logException(e);
        }

        return rVal;
    }

    @GET
    @Path("{category}/{categoryItem}/pages/{page}/projects/{projectRow}")
    @Produces(MediaType.APPLICATION_JSON)
    public ProjectMetadata serveCategoryBasedProjectJson(@PathParam("categoryItem") String categoryItemBase64,
                                                         @PathParam("page") Integer page,
                                                         @PathParam("projectRow") String projectRowBase64) {
        return getProjectMetadataEntity(decode64(projectRowBase64));
    }

    @GET
    @Path("{category}/{categoryItem}/pages/{page}/projects/{projectRow}/metadata/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    public ProjectMetadata serveCategoryBasedProjectMetadataJson(@PathParam("categoryItem") String categoryItemBase64,
                                                         @PathParam("page") Integer page,
                                                         @PathParam("projectRow") String projectRowBase64,
                                                                 @PathParam("key") String keyBase64) {
        return getProjectMetadataValue(decode64(projectRowBase64),decode64(keyBase64));
    }

    @GET
    @Path("{category}/{categoryItem}/pages/{page}/projects/{projectRow}/content")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response serveCategoryBasedProjectByteContentJson(@PathParam("categoryItem") String categoryItem,
                                                         @PathParam("page") Integer page,
                                                         @PathParam("projectRow") String projectRow) {
        String url = ScanWrapper.getProjectCodeUrl(projectRow);
        File file = new File(url);
        Response.ResponseBuilder response = Response.ok((Object) file);
        response.header("Content-Disposition", "attachment; filename=newfile.tgz");
        return response.build();
    }

    private final static int UUID_ELEMENT_INDEX = 4;
    private final static int UUID_ELEMENT_PREFIX_INDEX = 0;

    @GET
    @Path("{category}/{categoryItem}/pages/{page}/projects/{projectRow}/content/{type: code|metadata}")
    @Produces(MediaType.APPLICATION_JSON)
    public ProjectMetadata serveCategoryBasedProjectContentJson(@PathParam("category") String category,
                                                                @PathParam("categoryItem") String categoryItemBase64,
                                                                @PathParam("page") Integer page,
                                                                @PathParam("projectRow") String projectRowBase64,
                                                                @PathParam("type") String type) {
        ProjectMetadata p = null;

        try {
            // type = {code | metadata}
            String key = type+"FilePath";
            String projectRow = decode64(projectRowBase64);
            String uuid = projectRow.split("\\|")[UUID_ELEMENT_INDEX];

            String[] prefixAr = uuid.split("-")[UUID_ELEMENT_PREFIX_INDEX].split("");

            StringBuilder sb = new StringBuilder("/data/");
            for (String s : prefixAr){
                sb.append(s).append("/");
            }
            sb.append(uuid).append("/").append(uuid).append("_").append(type).append(".tgz");

            // for project metadata, the absolute path has too much
            // so we'll create one
            // "{category}/{categoryItem}/pages/{page}/projects/
            String projectPath = new StringBuilder(getBaseUrl()).append(MUSE).append(SLASH)
                    .append(CATEGORIES).append(SLASH).append(category).append(SLASH).append(categoryItemBase64).append(SLASH)
                    .append("pages").append(SLASH).append(page).append(SLASH)
                    .append(PROJECTS).append(SLASH).toString();
            p = new ProjectMetadata(projectPath, projectRow);
            HashMap<String, String> metadata = new HashMap<>();
            metadata.put(key, sb.toString());

            p.setMetadata(metadata);
            p.setKey(key);
        } catch (Exception e) {
            App.logException(e);
        }
        return p;
    }
    private final String MUSE = "muse";
    private final String CATEGORIES = "categories";
    private final String SLASH = "/";
    private final String PROJECTS = "projects";

}
