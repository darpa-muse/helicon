package com.twosixlabs.resources;

import com.twosixlabs.model.accumulo.ScanWrapper;
import com.twosixlabs.model.entities.*;
import com.twosixlabs.muse_utils.App;
import com.twosixlabs.muse_utils.Security;
import com.twosixlabs.view.Html;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.glassfish.jersey.server.ContainerRequest;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.twosixlabs.model.accumulo.ScanWrapper.*;
import static com.twosixlabs.view.Html.projectListResultView;

public class ResourceBase {
    @Context
    public HttpHeaders httpHeaders;

    protected String getApiKey() {
        List<String> headerVals = httpHeaders.getRequestHeader("apiKey");
        return headerVals != null? headerVals.get(0):null;
    }

    protected String getAbsoluteUrl() {
        return ((ContainerRequest) httpHeaders).getAbsolutePath().toString();
    }

    protected String getBaseUrl() {
        return ((ContainerRequest) httpHeaders).getBaseUri().toString();
    }

    protected UniqueCorpusMetadata setupUniqueMetadataValues(String key, HashSet<String> vals) {
        UniqueCorpusMetadata rVal = new UniqueCorpusMetadata();
        rVal.setValues(vals);

        return (UniqueCorpusMetadata) setupMetadataValues(rVal, key);
    }

    protected CorpusMetadata setupMetadataValues(String key, ArrayList<String> vals) {
        CorpusMetadata rVal = new CorpusMetadata();
        rVal.setArrayValues(vals);

        return (CorpusMetadata) setupMetadataValues(rVal, key);
    }

    protected CorpusMetadata setupMetadataValues(String key, ConcurrentHashMap<String, Integer> vals) {
        CorpusMetadata rVal = new CorpusMetadata();
        rVal.setMapValues(vals);

        return (CorpusMetadata) setupMetadataValues(rVal, key);
    }

    protected BaseMetadata setupMetadataValues(BaseMetadata metadata, String key) {
        metadata.setKey(key);
        metadata.setUrl(getAbsoluteUrl());
        return metadata;
    }

    private static String replaceLast(String text, String regex, String replacement) {
        return text.replaceFirst("(?s)(.*)" + regex, "$1" + replacement);
    }

    protected String RenderCategoryItemBasedProjects(String category, String categoryItemBase64,
                                                     String searchPrefixBase64, int page) {
        String rVal = null;

        try {
            //String category = new String(Base64.getUrlDecoder().decode(categoryBase64));

            // the breadcrumb trail places trailing '/' so remove it
            String categoryItemStripped = categoryItemBase64.endsWith("/") ?
                    categoryItemBase64.replaceAll(".$", "")
                    : categoryItemBase64;

            String searchPrefixStripped = searchPrefixBase64.endsWith("/") ?
                    searchPrefixBase64.replaceAll(".$", "")
                    : searchPrefixBase64;

            String categoryItem = new String(Base64.getUrlDecoder().decode(categoryItemStripped));
            String searchPrefix = new String(Base64.getUrlDecoder().decode(searchPrefixStripped));

            long start = System.currentTimeMillis();
            String path = getAbsolutePath();
            Boolean appendFlag = !path.contains("pages");

            String pageLink = appendFlag ? getAbsolutePath().concat("pages/")
                    : path;

            // is there a page number appended?
            Boolean endsWPages = pageLink.endsWith("pages/");
            Boolean endsWNum = pageLink.matches(".*/pages/([0-9]+)[/]$");
            Boolean endsWProj = pageLink.matches(".*/pages/([0-9]+)(/project|/projects/)$");

            String projectLink = "";
            if (endsWPages) {
                projectLink = pageLink + page + "/projects/";
            } else if (endsWNum) {
                projectLink = pageLink + "projects/";
            } else if (endsWProj) {
                projectLink = pageLink;
                pageLink = replaceLast(pageLink, "projects/", "");
            }

            HashMap<String, String> projectSet = null;
            if (category.equalsIgnoreCase("projectMetadataKey")) {
                projectSet = ScanWrapper.getProjectsWithMetadataKey(categoryItem,
                        ScanWrapper.PAGE_SIZE, page, "stargazers_count", true);
            } else {
                projectSet = ScanWrapper.getProjectsBasedOnColFamAndMetadataKey(category + "=" + categoryItem,
                        searchPrefix, ScanWrapper.PAGE_SIZE, page, "stargazers_count", true);
            }
            long duration = System.currentTimeMillis() - start;
            String caption = new StringBuilder("List of Projects using ")
                    .append(categoryItem).append(" (").append(duration).append(" msec)").toString();
            rVal = projectListResultView(caption, projectSet, projectLink,
                    page, pageLink, getBaseUrl());
        } catch (Exception e) {
            App.logException(e);
        }
        return rVal;
    }

    protected HashSet<Project> fetchCategoryItemBasedProject(String category, String categoryItem,
                                                             String searchPrefix, int page) {
        String path = getAbsolutePath();
        Boolean appendFlag = !path.contains("pages");

        String pageLink = appendFlag ? getAbsolutePath().concat("pages/")
                : path;
        String projectLink = appendFlag ? pageLink + page + "/projects/"
                : pageLink + "projects/";

        HashSet<Text> projectSet = ScanWrapper.getProjectsBasedOnColFam(category + "=" + categoryItem, searchPrefix, page);
        HashSet<Project> projects = new HashSet<Project>();
        projectSet.stream().forEach(r -> projects.add(new Project(projectLink, r.toString())));

        return projects;
    }

    String CAPTION_PREFIX = "Project Metadata for ";

    protected String renderProjectFromRow(String row) {
        String rVal = null;
        try {
            long start = System.currentTimeMillis();
            Iterator<Map.Entry<Key, Value>> projectSet = serveProjectFromRow(row);
            String captionPrefix = null;
            captionPrefix = CAPTION_PREFIX + row.split("\\|")[1];

            long duration = System.currentTimeMillis() - start;
            String caption = new StringBuilder(captionPrefix).append(" (").append(duration)
                    .append(" msec)").toString();

            rVal = Html.projectMetadataListView(getBaseUrl(), caption, projectSet); // [1] is the project name
        } catch (Exception e) {
            App.logException(e);
        }
        return rVal;
    }

    protected Iterator<Map.Entry<Key, Value>> serveProjectFromRow(String row) {
        return getProjectMetadataIterator(row);
    }

    protected String getAbsolutePath() {
        String path = ((ContainerRequest) httpHeaders).getAbsolutePath().toString();
        return (path.endsWith("/") ? path : path.concat("/"));
    }

    protected ProjectMetadata getProjectMetadataEntity(String projectRow){
        return getProjectMetadataEntity( null, projectRow);
    }

    protected ProjectMetadata getProjectMetadataEntity(String apiKey, String projectRow) {
        return getChoiceProjectMetadataEntity(apiKey, projectRow, PROJECT_METADATA);
    }

    protected HashMap<String, String> setupMetadataMap(String apiKey, String projectRow,
                                                       Text colF){
        Iterator<Map.Entry<Key, Value>> metaDataIterator = getProjectMetadataIterator(projectRow);
        HashMap<String, String> projectMetadataMap = new HashMap<>();
        while (metaDataIterator.hasNext()){
            Map.Entry<Key, Value> e = metaDataIterator.next();
            projectMetadataMap.put(e.getKey().getColumnQualifier().toString(),
                    e.getValue().toString());
        }
        try {
            if (Security.isMuseUser(apiKey)){
                projectMetadataMap.putAll(
                        ScanWrapper.readUserDataRecForWholeCF(apiKey,projectRow, colF, null)
                );
            }
        } catch (Exception e) {
            App.logException(e); // no need to throw this- it just skips getting priv data
        }
        return projectMetadataMap;
    }

    protected ProjectMetadata getChoiceProjectMetadataEntity(String apiKey, String projectRow, String type){
        ProjectMetadata projectMetadata = null;
        try {
            HashMap<String, String> projectMetadataMap = new HashMap<>();
            if (type.equalsIgnoreCase(PROJECT_METADATA)
                    || type.equalsIgnoreCase(ALL_METADATA)){
                projectMetadataMap.putAll(setupMetadataMap(apiKey, projectRow, PROJECT_METADATA_CF));
            }

            if (type.equalsIgnoreCase(PROJECT_FILES_METADATA)
                    || type.equalsIgnoreCase(ALL_METADATA)){
                projectMetadataMap.putAll(setupMetadataMap(apiKey, projectRow, PROJECT_FILES_METADATA_CF));
            }

            projectMetadata = new ProjectMetadata(getAbsoluteUrl(), projectRow);
            projectMetadata.setMetadata(projectMetadataMap);
        } catch (Exception e) {
            App.logException(e);
        }
        return projectMetadata;
    }

    protected ProjectMetadata getProjectMetadataValue(String projectRow, String key){
        ProjectMetadata pm = new ProjectMetadata(getAbsoluteUrl(),projectRow);
        HashMap<String,String> projectMetadataMap = new HashMap<>();
        projectMetadataMap.put(key, getProjectMetadataItem(projectRow,key));
        pm.setMetadata(projectMetadataMap);
        return pm;
    }

    // pull from the summary table
    protected static ConcurrentHashMap<String, Integer> getSpecificCategory(String colQual){
        ConcurrentHashMap<String, Integer> rVal = ScanWrapper.getSummaryTotal(colQual);

        return rVal;
    }

    protected static String decode64(String base64String){
        return new String(Base64.getDecoder().decode(base64String)).replace("\n","");
    }

}
