// When the user clicks on the search box, we want to toggle the search dropdown
function displayToggleSearch(e) {
  e.preventDefault();
  e.stopPropagation();

  closeDropdownSearch(e);
  
  if (idx === null) {
    console.log("Building search index...");
    prepareIdxAndDocMap();
    console.log("Search index built.");
  }
  const dropdown = document.querySelector("#search-dropdown-content");
  if (dropdown) {
    if (!dropdown.classList.contains("show")) {
      dropdown.classList.add("show");
    }
    document.addEventListener("click", closeDropdownSearch);
    document.addEventListener("keydown", searchOnKeyDown);
    document.addEventListener("keyup", searchOnKeyUp);
  }
}

//We want to prepare the index only after clicking the search bar
var idx = null
const docMap = new Map()

function prepareIdxAndDocMap() {
  const docs = [    
    {
      "title": "Introduction",
      "url": "/docs/",
      "content": "This page is a work in progress. It is dedicated to the latest release candidate version of Parquet4s. For a documentation of stable version 1.x of the library please refer to Readme. Introduction Parquet4s is a simple I/O for Parquet. Allows you to easily read and write Parquet files in Scala. Use just a Scala case class to define the schema of your data. No need to use Avro, Protobuf, Thrift or other data serialisation systems. You can use generic records if you don’t want to use the case class, too. Compatible with files generated with Apache Spark. However, unlike in Spark, you do not have to start a cluster to perform I/O operations. Based on official Parquet library, Hadoop Client and Shapeless (Shapeless is not in use in a version for Scala 3). Integrations for Akka Streams and FS2. Released for 2.12.x and 2.13.x and Scala 3.1.x."
    } ,      
    {
      "title": "Migration from 1.x",
      "url": "/docs/migration/",
      "content": "Migration from 1.x Be here soon."
    } ,    
    {
      "title": "Quick start",
      "url": "/docs/quick_start/",
      "content": "Quick start SBT libraryDependencies ++= Seq( \"com.github.mjakubowski84\" %% \"parquet4s-core\" % \"2.0.0-RC1\", \"org.apache.hadoop\" % \"hadoop-client\" % yourHadoopVersion ) Mill def ivyDeps = Agg( ivy\"com.github.mjakubowski84::parquet4s-core:2.0.0-RC1\", ivy\"org.apache.hadoop:hadoop-client:$yourHadoopVersion\" ) import com.github.mjakubowski84.parquet4s.{ ParquetReader, ParquetWriter, Path } case class User(userId: String, name: String, created: java.sql.Timestamp) val users: Iterable[User] = Seq( User(\"1\", \"parquet\", new java.sql.Timestamp(1L)) ) val path = Path(\"path/to/local/file.parquet\") // writing ParquetWriter.of[User].writeAndClose(path, users) // reading val parquetIterable = ParquetReader.as[User].read(path) try { parquetIterable.foreach(println) } finally parquetIterable.close() AWS S3 In order to connect to AWS S3 you need to define one more dependency: \"org.apache.hadoop\" % \"hadoop-aws\" % yourHadoopVersion Next, the most common way is to define following environmental variables: export AWS_ACCESS_KEY_ID=my.aws.key export AWS_SECRET_ACCESS_KEY=my.secret.key You may need to set some configuration properties to access your storage, eg. fs.s3a.path.style.access. Please follow documentation of Hadoop AWS for more details and troubleshooting. Passing Hadoop Configs Programmatically File system configs for S3, GCS, Hadoop, etc. can also be set programmatically to the ParquetReader and ParquetWriter by passing the Configuration to the ParqetReader.Options and ParquetWriter.Options case classes. import com.github.mjakubowski84.parquet4s.{ ParquetReader, ParquetWriter, Path } import org.apache.parquet.hadoop.metadata.CompressionCodecName import org.apache.hadoop.conf.Configuration case class User(userId: String, name: String, created: java.sql.Timestamp) val users: Iterable[User] = Seq( User(\"1\", \"parquet\", new java.sql.Timestamp(1L)) ) val hadoopConf = new Configuration() hadoopConf.set(\"fs.s3a.path.style.access\", \"true\") val writerOptions = ParquetWriter.Options( compressionCodecName = CompressionCodecName.SNAPPY, hadoopConf = hadoopConf ) ParquetWriter .of[User] .options(writerOptions) .writeAndClose(Path(\"path/to/local/file.parquet\"), users)"
    } ,      
  ];

  idx = lunr(function () {
    this.ref("title");
    this.field("content");

    docs.forEach(function (doc) {
      this.add(doc);
    }, this);
  });

  docs.forEach(function (doc) {
    docMap.set(doc.title, doc.url);
  });
}

// The onkeypress handler for search functionality
function searchOnKeyDown(e) {
  const keyCode = e.keyCode;
  const parent = e.target.parentElement;
  const isSearchBar = e.target.id === "search-bar";
  const isSearchResult = parent ? parent.id.startsWith("result-") : false;
  const isSearchBarOrResult = isSearchBar || isSearchResult;

  if (keyCode === 40 && isSearchBarOrResult) {
    // On 'down', try to navigate down the search results
    e.preventDefault();
    e.stopPropagation();
    selectDown(e);
  } else if (keyCode === 38 && isSearchBarOrResult) {
    // On 'up', try to navigate up the search results
    e.preventDefault();
    e.stopPropagation();
    selectUp(e);
  } else if (keyCode === 27 && isSearchBarOrResult) {
    // On 'ESC', close the search dropdown
    e.preventDefault();
    e.stopPropagation();
    closeDropdownSearch(e);
  }
}

// Search is only done on key-up so that the search terms are properly propagated
function searchOnKeyUp(e) {
  // Filter out up, down, esc keys
  const keyCode = e.keyCode;
  const cannotBe = [40, 38, 27];
  const isSearchBar = e.target.id === "search-bar";
  const keyIsNotWrong = !cannotBe.includes(keyCode);
  if (isSearchBar && keyIsNotWrong) {
    // Try to run a search
    runSearch(e);
  }
}

// Move the cursor up the search list
function selectUp(e) {
  if (e.target.parentElement.id.startsWith("result-")) {
    const index = parseInt(e.target.parentElement.id.substring(7));
    if (!isNaN(index) && (index > 0)) {
      const nextIndexStr = "result-" + (index - 1);
      const querySel = "li[id$='" + nextIndexStr + "'";
      const nextResult = document.querySelector(querySel);
      if (nextResult) {
        nextResult.firstChild.focus();
      }
    }
  }
}

// Move the cursor down the search list
function selectDown(e) {
  if (e.target.id === "search-bar") {
    const firstResult = document.querySelector("li[id$='result-0']");
    if (firstResult) {
      firstResult.firstChild.focus();
    }
  } else if (e.target.parentElement.id.startsWith("result-")) {
    const index = parseInt(e.target.parentElement.id.substring(7));
    if (!isNaN(index)) {
      const nextIndexStr = "result-" + (index + 1);
      const querySel = "li[id$='" + nextIndexStr + "'";
      const nextResult = document.querySelector(querySel);
      if (nextResult) {
        nextResult.firstChild.focus();
      }
    }
  }
}

// Search for whatever the user has typed so far
function runSearch(e) {
  if (e.target.value === "") {
    // On empty string, remove all search results
    // Otherwise this may show all results as everything is a "match"
    applySearchResults([]);
  } else {
    const tokens = e.target.value.split(" ");
    const moddedTokens = tokens.map(function (token) {
      // "*" + token + "*"
      return token;
    })
    const searchTerm = moddedTokens.join(" ");
    const searchResults = idx.search(searchTerm);
    const mapResults = searchResults.map(function (result) {
      const resultUrl = docMap.get(result.ref);
      return { name: result.ref, url: resultUrl };
    })

    applySearchResults(mapResults);
  }

}

// After a search, modify the search dropdown to contain the search results
function applySearchResults(results) {
  const dropdown = document.querySelector("div[id$='search-dropdown'] > .dropdown-content.show");
  if (dropdown) {
    //Remove each child
    while (dropdown.firstChild) {
      dropdown.removeChild(dropdown.firstChild);
    }

    //Add each result as an element in the list
    results.forEach(function (result, i) {
      const elem = document.createElement("li");
      elem.setAttribute("class", "dropdown-item");
      elem.setAttribute("id", "result-" + i);

      const elemLink = document.createElement("a");
      elemLink.setAttribute("title", result.name);
      elemLink.setAttribute("href", result.url);
      elemLink.setAttribute("class", "dropdown-item-link");

      const elemLinkText = document.createElement("span");
      elemLinkText.setAttribute("class", "dropdown-item-link-text");
      elemLinkText.innerHTML = result.name;

      elemLink.appendChild(elemLinkText);
      elem.appendChild(elemLink);
      dropdown.appendChild(elem);
    });
  }
}

// Close the dropdown if the user clicks (only) outside of it
function closeDropdownSearch(e) {
  // Check if where we're clicking is the search dropdown
  if (e.target.id !== "search-bar") {
    const dropdown = document.querySelector("div[id$='search-dropdown'] > .dropdown-content.show");
    if (dropdown) {
      dropdown.classList.remove("show");
      document.documentElement.removeEventListener("click", closeDropdownSearch);
    }
  }
}
