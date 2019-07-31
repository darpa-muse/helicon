package com.twosixlabs.view;

import com.twosixlabs.model.accumulo.ScanWrapper;
import com.twosixlabs.model.entities.MetadataValueStats;
import com.twosixlabs.muse_utils.App;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.math3.util.Precision;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Html {
    // Gets illegal chars out of project row
    private static String SIDE_NAV;
    static {
        try {
            InputStream strm = App.class.getClassLoader().getResourceAsStream("sidenav.html");
            try (Scanner scanner = new Scanner(strm, StandardCharsets.UTF_8.name())) {
                SIDE_NAV = scanner.useDelimiter("\\A").next();
            }
        } catch (Exception e) {
            StackTraceElement[] tes = e.getStackTrace();
            StringBuilder sb = new StringBuilder();
            for (StackTraceElement el : tes){
                sb.append(el.toString()).append("\n");
            }
            App.logger.info(sb.toString());
        }
    }
    private static String MUSE_BROWSER_JS;
    static {
        try {
            InputStream strm = App.class.getClassLoader().getResourceAsStream("musebrowser.js");
            try (Scanner scanner = new Scanner(strm, StandardCharsets.UTF_8.name())) {
                MUSE_BROWSER_JS = scanner.useDelimiter("\\A").next();
            }
        } catch (Exception e) {
            StackTraceElement[] tes = e.getStackTrace();
            StringBuilder sb = new StringBuilder();
            for (StackTraceElement el : tes){
                sb.append(el.toString()).append("\n");
            }
            App.logger.info(sb.toString());
        }
    }
    private static String MUSE_BROWSER_CSS;
    static {
        try {
            InputStream strm = App.class.getClassLoader().getResourceAsStream("musebrowser.css");
            try (Scanner scanner = new Scanner(strm, StandardCharsets.UTF_8.name())) {
                MUSE_BROWSER_CSS = scanner.useDelimiter("\\A").next();
            }
        }  catch (Exception e) {
            StackTraceElement[] tes = e.getStackTrace();
            StringBuilder sb = new StringBuilder();
            for (StackTraceElement el : tes){
                sb.append(el.toString()).append("\n");
            }
            App.logger.info(sb.toString());
        }
    }


    public static String listView(String caption, ConcurrentHashMap<String, Integer> categoryVals,
                                  String absPath, String baseUrl, boolean encode){
        StringBuilder rVal = new StringBuilder();
        try {
            rVal.append(generateProlog(baseUrl))
                    .append("<div id =\"resultsList\"")
                    .append("<caption style=\"text-align:left\"><b>").append(caption).append("</b></caption>")
                    .append("<ol type=\"1\">");

            TreeMap<String, Integer> list = new TreeMap<>();
            list.putAll(categoryVals);

            if (encode) {
                for (String s : list.keySet()) {
                    rVal.append("<li>");
                    rVal.append("<a onclick=\"on()\" href=\"").append(absPath).append(Base64.getUrlEncoder().encodeToString(s.trim().getBytes())).append("\"/>").append(s)
                            .append(" (").append(list.get(s)).append(")").append("</a>");
                    rVal.append("</li>");
                }
            }else{
                for (String s : list.keySet()) {
                    rVal.append("<li>");
                    rVal.append("<a onclick=\"on()\" href=\"").append(absPath).append(s.trim()).append("\"/>").append(s)
                            .append(" (").append(list.get(s)).append(")").append("</a>");
                    rVal.append("</li>");
                }
            }
            rVal.append("</ol></div>")
                    .append(getEpilog(absPath));
        }catch(Exception e){
            App.logException(e);
        }
        return rVal.toString();
    }
    private final static String EPILOG = "</div></body></html>";
    private final static String PROLOG = new StringBuilder("<html>") // 1.12.1/themes/base/jquery-ui.css
            .append("<head>")
/*
            .append("<script src=\"https://code.jquery.com/jquery-3.3.1.slim.min.js\" integrity=\"sha256-3edrmyuQ0w65f8gfBsqowzjJe2iM6n0nKciPUp8y+7E=\" crossorigin=\"anonymous\"></script>")
*/
            .append("<link rel=\"stylesheet\" href=\"http://code.jquery.com/ui/1.12.1/themes/smoothness/jquery-ui.css\" />")
            .append("<script src=\"http://code.jquery.com/jquery-1.12.4.min.js\"></script>")
            .append("<script src=\"http://code.jquery.com/ui/1.11.4/jquery-ui.js\"></script>")
            .append("<script src=\"https://use.fontawesome.com/79313f5b3f.js\"></script>")
            .append("<style>")
            .append(MUSE_BROWSER_CSS)
            .append("</style>")
            .append("</head>")
            .append("<body><div id=\"mainB\">").toString();

    private final static String menuBar(String restUriEntry){
        return (menuBarWQueryError(restUriEntry, null));
    }

    private final static String QUERY_PLACEHOLDER_TXT = "Enter search term or boolean expression here...";
    private final static String menuBarWQueryError(String restUriEntry, String placeholderTxt){
        if (placeholderTxt == null){
            placeholderTxt = QUERY_PLACEHOLDER_TXT;
        }
        StringBuilder sb = new StringBuilder("<div id=\"navBar\" class=\"navbar\">")
                .append("  <a href=\"").append(restUriEntry).append("muse/home").append("\">Home</a>")
                .append("  <a href=\"").append(restUriEntry).append("muse/categories").append("\">Browse</a>")
                .append("  <a href=\"").append(restUriEntry).append("muse/settings").append("\"><s>Settings</s></a>")
                .append("  <a onclick=\"openNav()\">&#9776; Help</a>")
                .append("</div>")
                .append("<textarea autocomplete=\"on\" wrap=\"off\" style=\"width:100%;\" class=\"queryString\" id=\"queryString\" ")
                .append("onkeyup=\"onSearchBoxKeystroke('").append(restUriEntry).append("',this.value)\"")
                .append(" autofocus rows=\"1\" cols=\"50\" placeholder=\"").append(placeholderTxt).append("\">")
                .append("</textarea><div id=qHolder />")
                .append("<div id = \"loader\"><div id=\"loaderText\">0</div></div>")
                .append("<ul class=\"breadCrumb\" id=\"breadCrumb\"></ul>")
                .append(getSideNav(restUriEntry));

        return sb.toString();
    }

    private final static String getSideNav(String restUriEntry){
        StringBuilder apiRefEndpoint = new StringBuilder(SIDE_NAV)
                .append("<a href =\"").append(restUriEntry).append("muse/help/api\"/>API Reference</a></div>");

        return apiRefEndpoint.toString();
    }
    public static String projectListResultView(String caption, HashMap<String, String> projectMap, String projectLinkBase, int page, String pageLinkBase, String baseUrl){
        // 5.0|091labs/lo-lo|2012-07-18|github|ed962fee-4e5c-426a-a111-abab9bca7d96
        int previous = page == 1? 1: page - 1;
        int next = page == 1? 2 : page + 1;
        // strip the page number if it exists
        pageLinkBase = pageLinkBase.matches(".*/pages/\\d+/$")? pageLinkBase.replaceAll("\\d+/$", "")
                : pageLinkBase;

        StringBuilder linkPrefix = new StringBuilder("<a onclick=\"on()\" href=\"")
                .append(pageLinkBase);
        String previousPageLink = page == 1? ""
                : linkPrefix.toString() + previous + "\">Previous Page</a>";
        String nextPageLink = projectMap.size() < ScanWrapper.PAGE_SIZE? ""
                : linkPrefix.toString() + next + "\">Next Page</a>";


        StringBuilder captionWithPage = new StringBuilder(caption)
                .append(" (Page ").append(page).append(")");
        int r =0;
        StringBuilder rVal = new StringBuilder(generateProlog(baseUrl))
                .append("<table id=\"navTable\" style=\"width:100%\">")
                .append("<caption style=\"text-align:center\"><b>").append(captionWithPage).append("</b></caption>")
                .append("<tr>").append("<td style=\"text-align:center\">").append(previousPageLink).append("</td>")
                .append("<td style=\"text-align:center\">").append(nextPageLink).append("</td>").append("</tr>")
                .append("</table>")

                .append("<table class=\"resultTable\" id=\"resultTable\" style=\"width:100%\">")
                .append("<tr>")
                .append("<th onclick=\"sortTable('resultTable',0)\">Name</th>")
                .append("<th onclick=\"sortTable('resultTable',1)\">Date</th>")
                .append("<th onclick=\"sortTable('resultTable',2)\">Repository</th>")
                .append("<th onclick=\"sortTable('resultTable',3)\">UUID</th>")
                .append("<th onclick=\"sortTable('resultTable',4)\">Rating<sup>")
                .append("<div class=\"tooltip\">?")
                .append("<span class=\"tooltiptext\">")
                .append("Based on Std<br>")
                .append("multiple<br>")
                .append("of Stargazer:<br>")
                .append("0 - 1...50% - 75%<br>")
                .append("1 - 2...75% - 87%<br>")
                .append("2 - 3...87% - 99%<br>")
                .append("  > 3...100%<br>")
                .append("</span></div></sup></th>")
                .append("<th onclick=\"sortTable('resultTable',5)\">Metadata</th>")
                .append("</tr>");

        double stargazerStd = App.getStargazersStats().getStd();
        Set<String> projectSet = projectMap.keySet();
        for (String t : projectSet) {
            String v = projectMap.get(t);
            double metadataVal = v.length() > 0 ? Double.parseDouble(v) : 0.0;
            double stdRatio = metadataVal/stargazerStd;
            double starWidth;
            if (stdRatio <= 1){// 50% - 75%
                starWidth = 75 + 25*(metadataVal - stargazerStd)/stargazerStd;
            }else if (stdRatio <= 2){// 75% - 87%
                starWidth = 75 + 12*(metadataVal - stargazerStd)/stargazerStd;
            }else if (stdRatio <= 3){// 87% - 99%
                starWidth = 75 + 12*(metadataVal - stargazerStd)/stargazerStd;
            }else{
                starWidth = 100;
            }

            String allElements[] = t.split("\\|"); // 2nd element is the name
            String rowElements[] = Arrays.copyOfRange(allElements, 1, allElements.length);
            rVal.append("<tr class = \"row").append(++r).append("\"/>");
            for (String e : rowElements) {
                rVal.append("<td>").append(e).append("</td>");
            }
            rVal.append("<td>")
                    .append("<div class=\"stars-outer\"><div class=\"stars-inner\"></div></div>")
                    .append("<sup><div class=\"tooltip\">?")
                    .append("<span class=\"tooltiptext\">")
                    .append(metadataVal + " stargazers")
                    .append("</span></div></sup>")
                    .append("</td>");
            String projectRow = t;
            try {
                rVal.append("<td><a href='")
                        .append(projectLinkBase).append(Base64.getUrlEncoder().encodeToString(projectRow.getBytes())).append("'/>")
                        .append("View</a>")
                        .append("<script>" +
                                "(function () {"
                                + "document.querySelector(\".row" + r
                                + " .stars-inner\").style.width = " + starWidth + "+'%';}());</script>")
                        .append("</td>")
                        .append("</tr>");
            } catch (Exception e) {
                App.logException(e);
            }
        }
        rVal.append("</table>")
                .append("<table id=\"navTable\" style=\"width:100%\">")
                .append("<caption style=\"text-align:center\"><b>").append(captionWithPage).append("</b></caption>")
                .append("<tr>").append("<td style=\"text-align:center\">").append(previousPageLink).append("</td>")
                .append("<td style=\"text-align:center\">").append(nextPageLink).append("</td>").append("</tr>")
                .append("</table>")
                .append(getEpilog(pageLinkBase));
        return rVal.toString();
    }

    private static String generateProlog(String path){
        return (PROLOG + menuBar(path));
    }
    private static String getEpilog() {
        return getEpilog("");
    }
    private static String getEpilog(String path){
        StringBuilder rVal =  new StringBuilder("<script type='text/javascript'>")
                .append("var autocompleteItems = [").append(App.CORPUS_AUTOSUGGEST_VALUES).append("];")
                .append(MUSE_BROWSER_JS)
                .append("populateBreadCrumb(\"" + path + "\"); ")
                .append("</script>")
                .append("<script>\n" +
                        "\n" +
                        " $(document).ready(function() {\n" +
                        "\n" +
                        "    $('.faq_question').click(function() {\n" +
                        "\n" +
                        "        if ($(this).parent().is('.open')){\n" +
                        "            $(this).closest('.faq').find('.faq_answer_container').animate({'height':'0'},500);\n" +
                        "            $(this).closest('.faq').removeClass('open');\n" +
                        "\n" +
                        "            }else{\n" +
                        "                var newHeight =$(this).closest('.faq').find('.faq_answer').height() +'px';\n" +
                        "                $(this).closest('.faq').find('.faq_answer_container').animate({'height':newHeight},500);\n" +
                        "                $(this).closest('.faq').addClass('open');\n" +
                        "            }\n" +
                        "\n" +
                        "    });\n" +
                        "\n" +
                        "});\n</script>")
                .append(EPILOG);


        return rVal.toString();
    }
    public static String projectMetadataListView(String baseUrl, String caption, Iterator<Map.Entry<Key, Value>> projectSet){
        StringBuilder rVal = new StringBuilder(generateProlog(baseUrl))
                .append("<table class=\"resultTable\" id=\"resultTable\" style=\"width:100%\">")
                .append("<caption style=\"text-align:left\"><b>").append(caption).append("</b></caption>")
                .append("<tr><th onclick=\"sortTable('resultTable',0)\">Metadata Key</th>")
                .append("<th onclick=\"sortTable('resultTable',1)\">Value</th></tr>");
        while (projectSet.hasNext()){
            Map.Entry<Key, Value> e = projectSet.next();

            rVal.append("<tr>");
            rVal.append("<td>").append(e.getKey().getColumnQualifier()).append("</td>");
            rVal.append("<td>").append(e.getValue()).append("</td>");
            rVal.append("</tr>");
        }
        rVal.append("</table>")
                .append(getEpilog());
        return rVal.toString();
    }

    public static String tableCategoriesWithCounts(String baseUrl, String itemUrl, String caption, HashMap<String, String> summaryVals, boolean encode) {
        StringBuilder rVal = new StringBuilder();

        try {
            rVal.append(generateProlog(baseUrl))
                    .append("<table class=\"categoryTable\" id=\"categoryTable\" style=\"width:30%\">")
                    .append("<caption style=\"text-align:center\"><b>").append(caption).append("</b></caption>")
                    .append("<tr><th onclick=\"sortTable('categoryTable',0)\">Category</th>")
                    .append("<th onclick=\"sortTable('categoryTable',1)\">Count</th></tr>");

            if (encode) {
                summaryVals.forEach((k, v) -> {
                    rVal.append("<tr>");
                    try {
                        rVal.append("<td>").append("<a onclick=\"on()\" href=\"").append(itemUrl)
                                .append(Base64.getUrlEncoder().encodeToString(k.getBytes())).append("\">").append(k).append("</td>");
                    } catch (Exception e) {
                        App.logException(e);
                    }
                    rVal.append("<td>").append(v).append("</td>");
                    rVal.append("</tr>");
                });
            }else{
                summaryVals.forEach((k, v) -> {
                    rVal.append("<tr>");
                    try {
                        rVal.append("<td>").append("<a onclick=\"on()\" href=\"").append(itemUrl)
                                .append(k).append("\">").append(k).append("</td>");
                    } catch (Exception e) {
                        App.logException(e);
                    }
                    rVal.append("<td>").append(v).append("</td>");
                    rVal.append("</tr>");
                });
            }
            rVal.append("</table>")
                    .append(getEpilog());
        } catch (Exception e) {
            App.logException(e);
        }
        return rVal.toString();
    }

    public static String welcomeStats(String baseUrl, String caption, ArrayList<MetadataValueStats> summaryVals) {
        StringBuilder rVal = new StringBuilder()
                .append("<br><br><table class=\"statTable\" id=\"statTable\" style=\"width:75%\">")
                .append("<caption style=\"text-align:center\"><b>").append(caption).append("</b></caption>")
                .append("<tr>")
                .append("<th onclick=\"sortTable('statTable',0)\">Name</th>")
                .append("<th onclick=\"sortTable('statTable',1)\">Mean</th>")
                .append("<th onclick=\"sortTable('statTable',2)\">Standard Dev.</th>")
                .append("<th onclick=\"sortTable('statTable',3)\">Min</th>")
                .append("<th onclick=\"sortTable('statTable',4)\">Max</th>")
                .append("<th onclick=\"sortTable('statTable',5)\">Sum</th>")
                .append("<th onclick=\"sortTable('statTable',6)\">Count</th>")
                .append("</tr>");
        for (MetadataValueStats m : summaryVals) {
            rVal.append("<tr>");
            rVal.append("<td>").append(m.getName()).append("</td>");
            rVal.append("<td>").append(Precision.round(m.getMean(), 2)).append("</td>");
            rVal.append("<td>").append(Precision.round(m.getStd(), 2)).append("</td>");
            rVal.append("<td>").append(Precision.round(m.getMin(), 2)).append("</td>");
            rVal.append("<td>").append(Precision.round(m.getMax(), 2)).append("</td>");
            rVal.append("<td>").append(Precision.round(m.getSum(), 2)).append("</td>");
            rVal.append("<td>").append(Precision.round(m.getCount(), 2)).append("</td>");
            rVal.append("</tr>");
        }
        rVal.append("</table>");

        rVal.append(getEpilog());
        return rVal.toString();
    }

    public static String overview() {
        return null;
    }

    public static String api(String baseUrl, ArrayList<String> apis) {
        StringBuilder rVal = null;
        try {
            rVal = new StringBuilder(generateProlog(baseUrl))
                    .append("<table class=\"resultTable\" id=\"resultTable\" style=\"width:100%\">")
                    .append("<caption style=\"text-align:Left\"><b>").append("API Reference").append("</b></caption>")
                    .append("<tr><th onclick=\"sortTable('resultTable', 0)\">Endpoint</th>")
                    .append("<th onclick=\"sortTable('resultTable',1)\">Description</th>")
                    .append("<th onclick=\"sortTable('resultTable',2)\">Media Type</th></tr>");
            for (String e : apis) {
                rVal.append("<tr>");
                rVal.append("<td>").append(e.split("\\|\\|")[0]).append("</td>");
                rVal.append("<td>").append(e.split("\\|\\|")[1]).append("</td>");
                rVal.append("<td>").append(e.split("\\|\\|")[2]).append("</td>");
                rVal.append("</tr>");
            }
            rVal.append("</table>")
                    .append(getEpilog());
        }catch (Exception e){
            App.logException(e);
        }
        return rVal.toString();
    }
}
