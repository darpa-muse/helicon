
function sortTable(table, n) {
  var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
  table = document.getElementById(table);
  switching = true;
  // Set the sorting direction to ascending:
  dir = "asc";
  /* Make a loop that will continue until
  no switching has been done: */
  var rv = on();

  while (switching) {
    // Start by saying: no switching is done:
    switching = false;
    rows = table.getElementsByTagName("TR");
    /* Loop through all table rows (except the
    first, which contains table headers): */
    for (i = 1; i < (rows.length - 1); i++) {
      // Start by saying there should be no switching:
      shouldSwitch = false;
      /* Get the two elements you want to compare,
      one from current row and one from the next: */
      x = rows[i].getElementsByTagName("TD")[n];
      y = rows[i + 1].getElementsByTagName("TD")[n];
      /* Check if the two rows should switch place,
      based on the direction, asc or desc: */
        var dataX = x.textContent; //.toLowerCase();
        var dataY = y.textContent; //.toLowerCase();
      if (isNaN(dataX) || isNaN(dataY)){
          if (dir == "asc") {
            if (dataX > dataY) {
              shouldSwitch= true;
              break;
            }
          } else if (dir == "desc") {
            if (dataX < dataY) {
              shouldSwitch= true;
              break;
            }
          }
      }else{
          if (dir == "asc") {
            if (parseFloat(dataX) > parseFloat(dataY)) {
              shouldSwitch= true;
              break;
            }
          } else if (dir == "desc") {
            if (parseFloat(dataX) < parseFloat(dataY)) {
              shouldSwitch= true;
              break;
            }
          }
      }
    }

    if (shouldSwitch) {
      /* If a switch has been marked, make the switch
      and mark that a switch has been done: */
      rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
      switching = true;
      // Each time a switch is done, increase this count by 1:
      switchcount ++;
    } else {
      /* If no switching has been done AND the direction is "asc",
      set the direction to "desc" and run the while loop again. */
      if (switchcount == 0 && dir == "asc") {
        dir = "desc";
        switching = true;
      }
    }
  }
  off();
}
function isBase64(str) {
    var base64Regex = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

    if (str == "projects"){
        return false;
    }else {
        return base64Regex.test(str);
    }
}
// form the breadcrumb by breaking up the path up by subpaths
function populateBreadCrumb(links){
    var link = window.location.href;
    document.getElementById("breadCrumb").innerHTML = "";
    var pivot = "/muse/";
    var pathParts = link.split(pivot);
    var home = pathParts[0] + pivot;
    var crumbParts = pathParts[1].split("/");
   	var path = home;

   	if (pathParts[1] != "home" && pathParts[1] != "search/"){
        for (var i = 0; i < crumbParts.length-1; i++){
            path += crumbParts[i] + "/";
            if (isBase64(crumbParts[i])){
                crumbList = "<li><a href=\"" + path + "\">" + atob(crumbParts[i]) +"</a></li>";
            }else{
                crumbList = "<li><a href=\"" + path + "\">" + crumbParts[i] +"</a></li>";
            }
            document.getElementById("breadCrumb").innerHTML += crumbList;
        }

        var fixedSubPath = crumbParts[crumbParts.length-1];
        if (isBase64(fixedSubPath)){
            fixedSubPath = atob(fixedSubPath);
        }
        crumbList = "<li><b>" + fixedSubPath + "</b></li>";
        document.getElementById("breadCrumb").innerHTML += crumbList;
   	} else{
        document.getElementById("breadCrumb").innerHTML += "<b>Welcome to Muse Corpus Browsing!</b>";
   	}
}

function openNav() {
    document.getElementById("sideNav").style.width = "250px";
    document.getElementById("mainB").style.marginLeft = "250px";
}

function closeNav() {
    document.getElementById("sideNav").style.width = "0";
    document.getElementById("mainB").style.marginLeft= "0";
}

function fillAnswer(q){
    var a1Text = "Answer: Help is available for an overview, this FAQ, and restful API for muse corpus bowsing ";
    var a2Text = "Answer: The home page displays various details about the corpus.  For instance, statistics on the categories to browse through along with items like minimum, average, and maximum size of projects in the corpus.";
    var a3Text = "Answer: Browse allows you to navigate starting with available categories:<ul>" +
    "<li>Clicking a category displays a list of that category (e.g., list of languages)</li>" +
    "<li>Clicking a specific category item displays a list of projects (e.g., list of java projects)</li>" +
    "<li>Clicking a specific project metadata link displays the stored metadata (project size, crawled date, etc.</li>" +
    "</ul>";
    var a4Text = "Answer: Advanced search is not yet implemented.";
    var ans;

    var currentText = document.getElementById(q).innerHTML;
    if (q == "a1Text"){
        ans = a1Text;
    } else if (q == "a2Text"){
        ans = a2Text;
    }else if (q == "a3Text"){
        ans = a3Text;
    }else if (q == "a4Text"){
        ans = a4Text;
    }
    if (currentText.length == 0){
        document.getElementById(q).innerHTML = ans;
    }else{
        document.getElementById(q).innerHTML = "";
    }
}

function fillStars(row, val) {
    var pcent = val + "%";
    document.querySelector(row + " .stars-inner").style.width=pcent;
}

var searchStrings = new Array();

function onSearchBoxKeystroke(urlBase, input) {
  var len = input.length;
  var key = window.event.keyCode;
  var elem = document.getElementById("qHolder");
  var qElem = document.getElementById("queryString");
  var queryStr = qElem.value;
  searchStrings.push(queryStr);

  if (key === 13 && elem.value === "" && queryStr.length > 1) {
    validateAndSearch(urlBase, btoa(queryStr)); // async call with callback
    event.preventDefault();
    searchStrings.length = 0;
    return false;
  }else if (key === 13 && queryStr.length ==1){
    qElem.value = "";
    event.preventDefault();
  }else{
    elem.value = "";
    ac();
  }

  function ac() {
    elem.value = "";
    function split( val ) {
      return val.split( / \s*/ );
    }
    function extractLast( term ) {
      return split( term ).pop();
    }

    return $( "#queryString" )
      .autocomplete({
        autoFocus: true,
        minLength: 0,
        source: function( request, response ) {
            var termLc = extractLast(request.term.toLowerCase());
          response( $.ui.autocomplete.filter(
            autocompleteItems.filter(function(word) {
                                    return word.toLowerCase().startsWith(termLc);
                                 }).sort().slice(0,6), termLc));
        },
        focus: function() {
          return false;
        },
        select: function( event, ui ) {
            var terms = split( this.value );

            // remove the current input
            var t = terms.pop();

            if (ui.item.value.length > 0){
                // add the selected item
                terms.push( ui.item.value );
                elem.value = ui.item.value;
            }else{
                terms.push(t);
                elem.value = "";
            }
            // add placeholder to get the comma-and-space at the end
            terms.push( "" );
            len = this.value.length;
            this.value = terms.join( " " );
            return false;
        }
      });
  };
 }

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// spinning counter while search takes place
    var loaderThreadCount = 0;
function on() {
    document.getElementById("loader").style.display = "block";
    document.getElementById("loader").className = "loader";
    document.getElementById("loaderText").innerHTML = ":-)";

    if (loaderThreadCount == 0){ // only need one worker thread

     function workerB() {
        setInterval(function() {
          postMessage(1);
        }, 1000);
      }

        // parsing out the code from the workerB function
      var code = workerB.toString();
      code = code.substring(code.indexOf("{")+1, code.lastIndexOf("}"));

      var blob = new Blob([code], {type: "application/javascript"});
      var worker = new Worker(URL.createObjectURL(blob));

      worker.onmessage = function(m) {
        var tElem = document.getElementById("loaderText");
        var t = tElem.innerHTML != ":-)"? parseInt(tElem.innerHTML) : 0;

        tElem.innerHTML = t+1;
      };

      loaderThreadCount++;
    }
return 1;
 }

 function off() {
     document.getElementById("loader").style.display = "none";
 }

 function validateAndSearch(baseUrl, queryStringBase64){
    var xhttp = new XMLHttpRequest();

    xhttp.onreadystatechange = function() {
        if (this.readyState == 4){
            if (this.status == 204){ // no content returned; query is valid
                on();
                window.location.href = baseUrl + "muse/search?q=" + queryStringBase64;
            }else if(this.status == 400){
                document.getElementById("queryString").rows = 5; // make some space for the error
                document.getElementById("queryString").value = "";
                document.getElementById("queryString").placeholder = decodeURIComponent(this.statusText).replace(/\+/g, " ");
            }
        }
    };

    xhttp.open("GET", baseUrl + "muse/search/validate/" + queryStringBase64, true);
    xhttp.send();
 }

