library(shiny)
library(shinythemes)
library(KoboconnectR)
library(dhis2r)
library(httr)
library(readr)
library(assertthat)
library(jsonlite)
library(purrr)
library(tidyr)
library(janitor)
library(lubridate)
library(dplyr)
library(openxlsx)
library(reactable)
library(auth0)

ui <- fluidPage(
  navbarPage(
    title = "Interoperaton",
    theme = shinytheme("yeti"),
    shiny::tabPanel(title = "Control",
                    fluidPage(
                      fluidRow(
                        column(width = 4,
                               wellPanel(
                                 textOutput("currentTime"),
                                 br(),
                                 h5("Step 1. Fetch updated data from Kobo Server"),
                                 actionButton(inputId = "kobo_data",
                                              label = "Kobo Fetch",
                                              icon("cloud-arrow-down",lib = "font-awesome"),
                                              class="btn btn-info"),
                                 br(),
                                 h5("Step 2. Kobo Aggregative Analytics Reconciliation"),
                                 actionButton(inputId = "kobo_agg",
                                              label = "Aggregative Analysis",
                                              icon("arrows-left-right-to-line",lib = "font-awesome"),
                                              class="btn btn-info")
                               ),
                               wellPanel(
                                 h5("Step 3. Merging analytics with HMIS/DHIS2"),
                                 actionButton(inputId = "d2_ouput",
                                              label = "Data Import",
                                              icon("file-arrow-up",lib = "font-awesome"),
                                              class="btn btn-primary")
                               )
                        ),
                        column(width = 8,
                               tabsetPanel(
                                 tabPanel("Message Log",
                                          verbatimTextOutput("log",placeholder = T)),
                                 tabPanel("Raw Data",
                                          uiOutput("kobo_raw"),
                                          downloadButton(outputId = "downloadraw",
                                                         label = "Download")),
                                 tabPanel("Indicators",
                                          uiOutput("kobo_ind"),
                                          downloadButton(outputId = "downloadind",
                                                         label = "Download"))
                               )
                        ),
                        column(width = 12,logoutButton())
                      )
                    )
    ),
    shiny::tabPanel(title = "Credentials",
                    fluidPage(
                      h4(code("You only need to create credentials ONCE!")),
                      p("Unless any credential data has changed."),
                      fluidRow(
                        column(width = 6,
                               style = "display: flex; align-items: center;",
                               a(href = "https://kf.kobotoolbox.org/accounts/login/",
                                 img(src = "kobotoolbox_image.png", height = 30, width = 100))
                        ),
                        column(width = 6,
                               style = "display: flex; align-items: center;",
                               a(href = "https://ntd-watch.itg.be/leprosy/",
                                 img(src = "dhis2_image.png", height = 30, width = 120))
                        ),
                        column(width = 6,
                               wellPanel(
                                 textInput(inputId = "kobo_url",
                                           label = "Kobo URL",
                                           value = "kf.kobotoolbox.org"),
                                 textInput(inputId = "kobo_user",
                                           label = "Kobo Username:"),
                                 passwordInput(inputId = "kobo_password",
                                               label = "Password:"),
                                 textInput(inputId = "formid",
                                           label = "Form Index:",
                                           value = "aAfsiPQF3vhxECnQNqhAoz"))
                        ),
                        column(width = 6,
                               wellPanel(
                                 textInput(inputId = "baseurl",
                                           label = "DHIS2 URL",
                                           value = "https://ntd-watch.itg.be/leprosy/"),
                                 textInput(inputId = "d2_user",
                                           label = "DHIS2 Username:"),
                                 passwordInput(inputId = "d2_password",
                                               label = "Password:"),
                                 textInput(inputId = "dataSet",
                                           label = "DataSet Uid:",
                                           value = "PdmRGdOM2rL"))
                        ),
                        column(width = 3,
                               actionButton(inputId = "create_cre",
                                            label="Create Credentials",
                                            icon("lock"),
                                            class="btn btn-danger")
                        ),
                        column(width = 12,
                               tableOutput(outputId = "cre_dt")
                        )
                      )
                    )
    )
  ),
  tags$style(".navbar {
               background-color: #005470; /* Change navbar background color */
               color: white; /* Change navbar text color */
             }")
)


# Define server logic required to draw a histogram
server <- function(input, output, session) {
  
  # Reactive value to store user credentials securely
  credentials <- reactiveValues(
    kobo = list(
      url = reactive({ input$kobo_url }),
      username = reactive({ input$kobo_user }),
      password = reactive({ input$kobo_password }),  
      formid = reactive({ input$formid })
    ),
    dhis2 = list(
      url = reactive({ input$baseurl }),
      username = reactive({ input$d2_user }),
      password = reactive({ input$d2_password }),  
      dataSet = reactive({ input$dataSet })
    )
  )
  
  cre_created <- reactiveVal(FALSE)
  # Cre
  observeEvent(input$create_cre, {
    if (!cre_created()) {
      
      # Prepare credentials data frame
      credentials_df <- data.frame(
        Platform = c("Kobo", "DHIS2"),
        URL = c(credentials$kobo$url(), credentials$dhis2$url()),
        Username = c(credentials$kobo$username(), credentials$dhis2$username()),
        DataID = c(credentials$kobo$formid(), credentials$dhis2$dataSet()),
        Password = c(credentials$kobo$password(), credentials$dhis2$password())
      )
      
      test_kobo<-get_kobo_token(
        url = credentials_df[1,"URL"],
        uname = credentials_df[1,"Username"],
        pwd = credentials_df[1,"Password"],
        encoding = "UTF-8")

      loginDHIS2<-function(baseurl,username,password) {
        url<-paste0(baseurl,"api/me")
        r<-GET(url,authenticate(username,password))
        assert_that(r$status_code == 200L) }
      
      test_d2<-loginDHIS2(credentials_df[2,"URL"],
                          credentials_df[2,"Username"],
                          credentials_df[2,"Password"])
      login<-c()
      if (names(test_kobo)=="detail") {login<-"Fail"} else 
      {login<-"Success"}
      if (test_d2) {append(login,"Success",after = length(login))} else 
      {append(login, ifelse(test_d2, "Success", "Fail"), after = length(login))}
      
      # Review credential data in a summary table
      
      credentials_df_re(bind_cols(credentials_df[,1:4],Status))
 
      saveRDS(credentials_df, file = "credentials_data.rds")
      
      cre_created(TRUE)
    }
  })
  
  captured_text <- reactiveVal(character())
  addMessage <- function(message) {
    current_log <- captured_text()
    updated_log <- c(current_log, message)
    captured_text(updated_log)
  }
  
  kobo_fetched <- reactiveVal(FALSE)
  # Kobo Fetch
  observeEvent(input$kobo_data, {
    if (!kobo_fetched()) {
      withProgress({
        message("Step 1.Fetching data from Kobo")
        
        cre_df<-readRDS("credentials_data.rds")
        
        kobo_url<-cre_df[1,"URL"]
        kobo_username<-cre_df[1,"Username"]
        kobo_password<-cre_df[1,"Password"]
        formid<-cre_df[1,"DataID"]
        
        incProgress(0, detail = "Now accessing the Kobo server...")
        
        kobo_console<-capture.output({
          new_export_url<-KoboconnectR::kobo_export_create(url=kobo_url, 
                                                           uname=kobo_username, 
                                                           pwd=kobo_password,
                                                           assetid=formid,
                                                           type= "csv", all="false", lang="_default",
                                                           hierarchy="false", include_grp="true",grp_sep="/")}) #Create export
        
        incProgress(0.4, detail = "Now fetching the data...")
        
        df<-httr::GET(new_export_url, httr::authenticate(user=kobo_username, password=kobo_password)) # Download
        df<-httr::content(df, type="raw",encoding = "UTF-8") # Extract raw content
        
        incProgress(0.6, detail = "Downloading...")
        
        writeBin(df, "data.csv") # Write in local
        kobo<-read.csv("data.csv", sep=";")
        
        kobo_console <- gsub("\r", "", kobo_console)
        kobo_console <- kobo_console[1:2]
        addMessage("### Step 1.Fetching data from Kobo server ###")
        addMessage(kobo_console)
        
        msg<-capture.output(message("Total downloded: ",nrow(kobo)," row data","\n"))
        addMessage(msg)
      })
      kobo_fetched(TRUE) # Set flag to TRUE
    }
  })
  
  new_list_re<-reactiveVal(NULL)
  
  kobo_analyzed <- reactiveVal(FALSE)
  # Kobo Analysis
  observeEvent(input$kobo_agg, {
    if (!kobo_analyzed()) {
      withProgress({
        message("Step 2. Kobo data aggregation")
        
        load("temp.RData")
        load("ou_list.RData")
        load("ds_list.RData")
        cre_df<-readRDS("credentials_data.rds")
        
        incProgress(0, detail = "Now cleaning/processing the row data...")
        
        last2y <- format(seq(Sys.Date() - years(4), Sys.Date(), by = "year"), "%Y-%m")
        kobo<-read.csv("data.csv", sep=";")
        kobo<-kobo %>%
          dplyr::mutate(reg_year=year(registration_date),
                        class=case_when(leprosyclass=="1"~"MB",
                                        leprosyclass=="2"~"PB"),
                        reg_mth=format_ISO8601(ymd(registration_date), precision = "ym"),
                        out_mth=format_ISO8601(ymd(outcome_date), precision = "ym"))
        
        addMessage("### Step 2.Kobo data aggregation (calculating indicators) ###")
        
        addMessage(paste("Warning:",sum(duplicated(kobo$uid)==T),"data with dupicated UID"))
        addMessage(paste("Warning:",sum(kobo$reg_year!=year(Sys.Date())&!kobo$reg_mth %in% last2y),
                         " data","with registration date not within this year or the last 2 years"))
        
        kobo_clean<-kobo %>%
          filter(reg_year==year(Sys.Date())|reg_mth %in% last2y )%>% 
          dplyr::distinct(uid,.keep_all = TRUE) %>% 
          left_join(ou_list,by = join_by(tole == name))
        
        kobo_dt<-kobo
        kobo_dt$Data_Issue[duplicated(kobo$uid)]<-"Duplicated"
        kobo_dt$Data_Issue[kobo$reg_year!=year(Sys.Date())&!kobo$reg_mth %in% last2y]<-"Invalid Date/Outside Evaluation Period"
        kobo_dt$registration_date <- as.Date(kobo_dt$registration_date)
        kobo_dt<-kobo_dt %>% 
          select(uid,province,district,municipality,ward_no,registration_number,registration_date,
                 leprosyclass,case_detection_method,registered_as,lepra_reaction,skin_smear_test,
                 i_who_disability,ehf_score,contact_pep,treatment_outcome,outcome_date) %>% 
          arrange(province,registration_date)
        
        kobo_dt_re(kobo_dt)
        
        addMessage("You can review the [Kobo_Raw_Data] in the Table table")
        addMessage("The data issue can be filtered with variable [Data_Issue]")
        
        incProgress(0.4, detail = paste("Now processing:",nrow(kobo_clean),"clean data"))
        
        ###Computing aggregation analytics by "tole" or corresponding OUs in DHIS2
        
        now<-format_ISO8601(Sys.Date(), precision = "ym")
        
        ### Update all/replace 
          
        incProgress(0.6, detail = "Now computing indicators...")
          
        table<-janitor::tabyl(kobo_clean,id,class,reg_mth) ### the updated Kobo data
          
        new_list<-table
         
      #save(new_list, file="new_list.RData") 
      new_list_re(new_list)
      print(new_list_re())
      print(names(new_list_re()))
      d2_period<-names(new_list_re())
      print(d2_period[1])
      })
      
      kobo_analyzed(TRUE) # Set flag to TRUE
    }
  })
  
  dhis2_imported <- reactiveVal(FALSE)
  ### Import
  observeEvent(input$d2_ouput, {
    if (!dhis2_imported()) {
      withProgress({
        message("Step 3.DHIS2 Database Import")
        
        cre_df<-readRDS("credentials_data.rds")
        #load("new_list.RData")
        load("temp.RData")
        load("ou_list.RData")
        load("ds_list.RData")
        
        incProgress(0, detail = "Accessing DHIS2 server...")
        
        baseurl<-cre_df[2,"URL"]
        d2_username<-cre_df[2,"Username"]
        d2_password<-cre_df[2,"Password"]
        dataSet<-cre_df[2,"DataID"]
        
        loginDHIS2<-function(baseurl,username,password) {
          url<-paste0(baseurl,"api/me")
          r<-GET(url,authenticate(username,password))
          assertthat::assert_that(r$status_code == 200L) }
        
        loginDHIS2(baseurl,d2_username,d2_password)
        
        addMessage("### Step 3.DHIS2 data integration ###")
        
        incProgress(0.1, detail = "Login Success!")
        incProgress(0.2, detail = "Importing data...")
        
        d2_period<-names(new_list_re())
        
        output_list<-list()
        fail_list<-list()
        
        for (qt in d2_period){
          dt<-new_list_re()[[qt]]
          d2_ou<-dt$id
          output_dts<-list()
          fail_dts<-list()
          for (uid in d2_ou){
            url<-paste0(baseurl,
                        "api/dataValueSets.json?",
                        "dataSet=",dataSet,
                        "&period=",qt,
                        "&orgUnit=",uid)
            dtaVal<-temp
            dtaVal$dataValues$period<-qt
            dtaVal$dataValues$orgUnit<-uid
            ###MB Data
            purrr::pluck(dtaVal,1,"value",1)<-dt$MB[dt$id==uid]
            ###PB Data
            purrr::pluck(dtaVal,1,"value",2)<-dt$PB[dt$id==uid]
            
            resp<-httr::POST(url,body=jsonlite::toJSON(dtaVal,auto_unbox = TRUE), httr::content_type_json())
            
            if(resp$status_code==200){
              output_dts[[uid]]<-dtaVal$dataValues[c("dataElement","period","value")]} else {
                fail_dts[[uid]]<-dtaVal$dataValues[c("dataElement","period","value")]
              } #check 200 response
            output_list[[qt]] <- output_dts
            fail_list[[qt]] <- fail_dts
            
            incProgress((0.2+(0.7/length(d2_ou))), detail = "Importing data...")
            
          }
        }
        
        incProgress(1, detail = "DHIS2 data value import complete!")
        
        merged_s<-data.frame()
        for (i in 1:length(output_list)){
          og<-names(output_list[[i]])
          if(length(output_list[[i]])>0){
            for (j in 1:length(output_list[[i]])){
              output_list[[i]][[j]]$og<-og[j]
              output_list[[i]][[j]]$Status<-"Success"
            }}
          merged_s<-rbind(merged_s,rlist::list.stack(output_list[[i]]))
        }
        
        merged_f<-data.frame()
        for (i in 1:length(fail_list)){
          og<-names(fail_list[[i]])
          if(length(fail_list[[i]])>0){
            for (j in 1:length(fail_list[[i]])){
              fail_list[[i]][[j]]$og<-og[j]
              fail_list[[i]][[j]]$Status<-"Fail"
            }}
          merged_f<-rbind(merged_f,rlist::list.stack(fail_list[[i]]))
        }
        
        ds<-ds_list %>% 
          filter(type=="AGGREGATE") %>% 
          select(name,id)
        
        if(nrow(merged_s)>0){
          merged_s<-left_join(merged_s,ou_list[,c("id","name")],join_by(og==id))}
        
        if(nrow(merged_s)>0){
          merged_s<-left_join(merged_s,ds,join_by(dataElement==id)) %>% 
            select(-dataElement,-og)
          colnames(merged_s)<-c("Period","Update_Value","Status","Municipality","Indicator")
        }
        
        if(nrow(merged_f)>0){
          merged_f<-left_join(merged_f,ou_list[,c("id","name")],join_by(og==id))}
        
        if(nrow(merged_f)>0){
          merged_f<-left_join(merged_f,ds,join_by(dataElement==id)) %>% 
            select(-dataElement,-og)
          colnames(merged_f)<-c("Period","Update_Value","Status","Municipality","Indicator")
        }
        
        merged_df<-bind_rows(merged_s,merged_f)
        
        ind_dt<-pivot_wider(merged_df,names_from = Indicator,values_from = Update_Value)
        ind_dt_re(ind_dt)
        
        addMessage(paste("Notice:",sum(ind_dt$Status=="Success"),"/",nrow(ind_dt),"Success Import!!!"))
        
        addMessage(paste("Warning:",sum(ind_dt$Status=="Fail"),"/",nrow(ind_dt),"Success Import!!!"))
        
      })
      
      dhis2_imported(TRUE) # Set flag to TRUE
    }
    
  })
  
  ind_dt_re <- reactiveVal(NULL)
  
  kobo_dt_re <- reactiveVal(NULL)
  
  credentials_df_re<-reactiveVal(NULL)
  
  output$cre_dt <- renderTable({
    if (is.null(credentials$kobo$url())) {
      return(NULL)
    } else {
      credentials_df_re()
    }
  })
  
  output$kobo_ind <- renderUI({
    if (is.null(ind_dt_re())) {
      tagList(
        h3("Waiting for data..."),
        p("Data will be displayed here once available.")
      )
    } else { reactable(ind_dt_re(),
              showPageInfo = FALSE, defaultPageSize = 5,
              filterable = TRUE, minRows = 10, 
              compact = TRUE,
              searchable = TRUE)
    }
  })
  
  output$kobo_raw <- renderUI({
    # Check if data exists
    if (is.null(kobo_dt_re())) {
      # If data is not available, display placeholder message
      tagList(
        h3("Waiting for data..."),
        p("Data will be displayed here once available.")
      )
    } else { reactable(kobo_dt_re(),
              showPageInfo = FALSE, defaultPageSize = 5,
              filterable = TRUE, minRows = 10, 
              compact = TRUE,
              searchable = TRUE)
    }
  })
  
  
  output$downloadind <- downloadHandler(
    filename = function() {
      paste0("Import_log_", format(Sys.Date(), "%Y-%m-%d"), ".xlsx")
    },
    content = function(file) {
      # Save the Excel file
      writexl::write_xlsx(ind_dt_re(), file)
    }
  )
  
  output$downloadraw <- downloadHandler(
    filename = function() {
      paste("KoboRawData_", Sys.Date(),".xlsx", sep = "")
    },
    content = function(file) {
      writexl::write_xlsx(kobo_dt_re(), file)
    }
  )
  
  observe({
    output$log <- renderPrint({
      captured_text()
    })
  })
  
  
}

auth0::shinyAppAuth0(ui = ui, server = server)

