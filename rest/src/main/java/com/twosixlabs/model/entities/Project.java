package com.twosixlabs.model.entities;

import com.twosixlabs.muse_utils.App;

import java.util.Base64;
import java.util.regex.Pattern;

public class Project extends BaseMetadata{
    public Project(){}
    public Project(String s) {

    }

    public String getMiniMetadataUrl() {
        return miniMetadataUrl;
    }

    public void setMiniMetadataUrl(String miniMetadataUrl) {
        this.miniMetadataUrl = miniMetadataUrl;
    }

    private String miniMetadataUrl;
    private String metadataUrl;
    private String codeUrl;

    public String getRowId() {
        return rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }

    private String rowId;
    public String getCodeUrl() {
        return codeUrl;
    }

    public void setCodeUrl(String contentUrl) {
        this.codeUrl = contentUrl;
    }

    // projectSubPath is the rowId
    public Project(String projectLink, String rowId){
        try {
            this.rowId = rowId;
            this.url = projectLink;
            // let's build these urls
            String parts[] = projectLink.split(Pattern.quote(rowId)); // parts[0] is the .../project path

            this.miniMetadataUrl = parts[0] + Base64.getEncoder().encodeToString(rowId.getBytes());
            this.metadataUrl = this.miniMetadataUrl + "/content/metadata";
            this.codeUrl = this.miniMetadataUrl + "/content/code";
        } catch (Exception e) {
            App.logException(e);
        }
    }

    public String getMetadataUrl(){
        return metadataUrl;
    }

    public void setMetadataUrl(String url){
        this.metadataUrl = url;
    }

    @Override
    public int hashCode() {
        return rowId.hashCode();
    }
}
