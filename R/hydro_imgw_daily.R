#' Daily hydrological data
#'
#' Downloading daily hydrological data from the danepubliczne.imgw.pl collection.
#'
#' @param year vector of years (e.g., 1966:2000)
#' @param coords logical; add coordinates of the stations
#' @param station character (CAPS) or numeric ID of station(s)
#' @param col_names "short" (default), "full", or "polish"
#' @param allow_failure logical; proceed on failure (default TRUE)
#' @param ... additional options; np. `save_dir` do zapisu plików na dysk oraz argumenty do skracania kolumn
#' @importFrom XML readHTMLTable
#' @importFrom utils unzip
#' @return data.frame with daily hydrological data
#' @export
hydro_imgw_daily <- function(year,
                             coords = FALSE,
                             station = NULL,
                             col_names = "short",
                             allow_failure = TRUE,
                             ...) {
  if (allow_failure) {
    tryCatch(
      hydro_imgw_daily_bp(year, coords, station, col_names, ...),
      error = function(e) {
        message("Problems with downloading data. Run with allow_failure = FALSE to see more details")
      }
    )
  } else {
    hydro_imgw_daily_bp(year, coords, station, col_names, ...)
  }
}

#' @keywords internal
#' @noRd
hydro_imgw_daily_bp = function(year,
                               coords,
                               station,
                               col_names,
                               ...) {
  translit = check_locale()
  base_url = "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_hydrologiczne/"
  interval = "daily"
  interval_pl = "dobowe"
  
  # opcjonalny katalog docelowy (bez zmiany sygnatury)
  dots <- list(...)
  save_dir <- dots$save_dir  # np. "D:/IMGW"
  
  # initiate empty objects:
  all_data = NULL
  codz_data = NULL
  zjaw_data = NULL
  
  temp = tempfile()
  test_url(link = paste0(base_url, interval_pl, "/"), output = temp)
  a = readLines(temp, warn = FALSE)
  
  ind = grep(readHTMLTable(a)[[1]]$Name, pattern = "/")
  catalogs = as.character(readHTMLTable(a)[[1]]$Name[ind])
  catalogs = gsub(x = catalogs, pattern = "/", replacement = "")
  catalogs = catalogs[catalogs %in% as.character(year)]
  
  if (length(catalogs) == 0) {
    stop("Selected year(s) is/are not available in the database.", call. = FALSE)
  }
  meta = hydro_metadata_imgw(interval)
  
  for (i in seq_along(catalogs)) {
    catalog = catalogs[i]
    
    temp = tempfile()
    test_url(link = paste0(base_url, interval_pl, "/", catalog), output = temp)
    b = readLines(temp, warn = FALSE)
    
    files_in_dir = readHTMLTable(b)[[1]]$Name
    ind = grep(files_in_dir, pattern = "zip")  # (pozostawione bez zmian)
    codz_files = grep(x = files_in_dir, pattern = "codz", value = TRUE)
    zjaw_files = grep(x = files_in_dir, pattern = "zjaw", value = TRUE)
    iterator = c(codz_files, zjaw_files)
    
    # jeśli zapisujemy na dysk – przygotuj podkatalog roku
    if (!is.null(save_dir)) {
      out_dir_year <- file.path(save_dir, catalog)
      if (!dir.exists(out_dir_year)) dir.create(out_dir_year, recursive = TRUE)
    }
    
    for (j in seq_along(iterator)) {
      fn <- iterator[j]
      address = paste0(base_url, interval_pl, "/", catalog, "/", fn)
      
      # gdzie pobrać plik (TMP albo na dysk)
      if (is.null(save_dir)) {
        tmp_file <- tempfile()
        test_url(link = address, output = tmp_file)
        message(sprintf("Pobrano (TMP): %s", fn))
        download_path <- tmp_file
      } else {
        dest_path <- file.path(out_dir_year, basename(fn))
        test_url(link = address, output = dest_path)
        message(sprintf("Pobrano: %s -> %s", fn, dest_path))
        download_path <- dest_path
      }
      
      temp2 = tempfile()
      # ZIP -> rozpakuj do TMP na potrzeby wczytania; CSV -> czytaj bezpośrednio
      if (grepl("\\.zip$", fn, ignore.case = TRUE)) {
        utils::unzip(zipfile = download_path, exdir = temp2)
        file_path = paste(temp2, dir(temp2), sep = "/")[1]
      } else {
        file_path = download_path
      }
      
      if (grepl(x = fn, "codz")) {
        data1 = imgw_read(translit, file_path)
        if (ncol(data1) == 9) {
          data1$flow = NA
          data1 = data1[, c(1:7, 10, 8:9)]
        }
        colnames(data1) = meta[[1]][, 1]
        codz_data = rbind(codz_data, data1)
      }
      
      if (grepl(x = fn, "zjaw")) {
        data2 = imgw_read(translit, file_path)
        colnames(data2) = meta[[2]][, 1]
        zjaw_data = rbind(zjaw_data, data2)
      }
    } # end files loop
    
    all_data[[length(all_data) + 1]] = merge(codz_data, zjaw_data,
                                             by = intersect(colnames(codz_data), colnames(zjaw_data)),
                                             all.x = TRUE)
  } # end years loop
  
  all_data = do.call(rbind, all_data)
  all_data[all_data == 9999] = NA
  all_data[all_data == 99999.999] = NA
  all_data[all_data == 99.9] = NA
  all_data[all_data == 999] = NA
  
  if (coords) {
    all_data = merge(climate::imgw_hydro_stations, all_data,
                     by.x = "id",
                     by.y = "Kod stacji",
                     all.y = TRUE)
  }
  
  if (!is.null(station)) {
    if (is.character(station)) {
      all_data = all_data[substr(all_data$`Nazwa stacji`, 1, nchar(station)) == station, ]
      if (nrow(all_data) == 0) {
        stop("Selected station(s) is not available in the database.", call. = FALSE)
      }
    } else if (is.numeric(station)) {
      all_data = all_data[all_data$`Kod stacji` %in% station, ]
      if (nrow(all_data) == 0) {
        stop("Selected station(s) is not available in the database.", call. = FALSE)
      }
    } else {
      stop("Selected station(s) are not in the proper format.", call. = FALSE)
    }
  }
  
  all_data = all_data[do.call(order, all_data[grep(x = colnames(all_data), "Nazwa stacji|Rok hydro|w roku hydro|Dzie")]), ]
  yy_ind = grep(x = colnames(all_data), "Rok hydrologiczny")
  mm_ind = grep(x = colnames(all_data), "kalendarzowy")
  dd_ind = grep(x = colnames(all_data), "Dzie")
  data_df = all_data[, c(yy_ind, mm_ind, dd_ind)]
  data_df$yy = ifelse(data_df[, 2] >= 11, data_df[, 1] - 1, data_df[, 1])
  all_data$Data = as.Date(ISOdate(year = data_df$yy, month = data_df[, 2], day = data_df[, 3]))
  all_data = all_data[, c(1:3, ncol(all_data), 4:(ncol(all_data) - 1)), ]
  
  all_data = hydro_shortening_imgw(all_data, col_names = col_names, ...)
  return(all_data)
}

