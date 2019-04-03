/*
Customizations for Rmarkdown reports.

These can be thought of as plugins. Some will be incorporated by default,
and others can be called as necessary in the main Rmd file.
*/

var rmdCustomJS = (function() {
    /*
    Convert headers to hyperlinks linking to themselves, so as to easily be
    able to grab links to specific sections.
    */
    function addHeaderLinks() {
        $("h1,h2,h3,h4,h5").filter(function(i) {
            // Note: tried selecting "div.section > h1,h2,h3,h4,h5", but 
            // this was pulling in the initial h4 date heading.
            return $(this).parent().hasClass("section");
        }).wrap(function() {
            var sectionref = $(this).parent().attr("id");
            // Add a custom class for additional styling control.
            return "<a class='header-link' href='#" + sectionref + "'></a>";
        });
    }

    /*
    Selective code folding:
    - code blocks with class "code-show" should start with the code expanded
    - code blocks with class "code-hide" should start with the code hidden
    regardless of document-wide code folding default.
    Class can be set using the `class.source` chunk option.
    */
    function toggleSelectiveCodeFolding() {
        $("pre.code-show").each(function() {
            $(this).parent().collapse("show");
        });
        $("pre.code-hide").each(function() {
            $(this).parent().collapse("hide");
        });
    }

    /*
    Convert top-level ('level1') sections to tabs.
    This is done by wrapping in a 'level0' tabset div and building the tabs.
     */
    function convertLevel1ToTabs() {
        $("div.section.level1").wrapAll(
            "<div id='level0-tabset'" +
                " class='section level0 tabset tabset-sticky' />"
        );
        window.buildTabsets("TOC");
    }

    /*
    Apply custom styling to tabs that should come across as de-emphasized.
    Supply a list of the element IDs for the tabs to customize.
    */
    function applyDeemphasizedTabStyling(tabIds) {
        tabIds.forEach(function(tabId) {
            $(".tabset a[href='#" + tabId + "']").addClass("tab-deemph");
        });
    }

    return {
        /*
        Run the customizations that should be applied in all Rmd documents.
        */
        applyDefaultCustomizations: function() {
            $(document).ready(function() {
                addHeaderLinks();
                toggleSelectiveCodeFolding();
            });
        },
        /* Convert top-level sections to tabs.  */
        createTopLevelTabs: function() {
            $(document).ready(function() {
                convertLevel1ToTabs();
            });
        },
        /*
        Apply styling to the given tab IDs to make them appear de-emphasized.
        */
        deemphasizeTabs: function(tabIds) {
            $(document).ready(function() {
                applyDeemphasizedTabStyling(tabId);
            });
        }
    }
})();

rmdCustomJS.applyDefaultCustomizations();

