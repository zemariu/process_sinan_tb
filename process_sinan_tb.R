#'-------------------------------------------------------------------
#' Curso: Epidemiologia descritiva aplicada à tuberculose
#' Função única: carrega .dbc/.csv de TB e processa/rotula as variáveis
#' Autor: José Mário Nunes da Silva Ph.d
#' Contato: zemariu@usp.br
#'-------------------------------------------------------------------
#'Como utilizar:
#'-------------------------------------------------------------------
#'
#'library(read.dbc); library(tidyverse)  # apenas para seu script principal
#'
#'source("process_tb.R")
#'
#'setwd("~/CursoR") # exemplo do diretório
#'
#' bancos_tb <- process_sinan_tb(
#' anos = 2010:2023,
#' directory = "BancosTB",
#' quiet = FALSE)
#'
#' Exemplos rápidos de verificação:
#' table(bancos_tb$UF_RES, useNA="ifany")[1:5]
#' table(bancos_tb$REGIAO_RES, useNA="ifany")
#' head(bancos_tb[, c("SG_UF_COD","SG_UF","UF_RES","REGIAO_RES","SG_UF_NOT_COD","SG_UF_NOT","UF_NOT","REGIAO_NOT")])
#' table(bancos_tb$CAPITAL_RES, useNA="ifany")  # NA padrão = não-capital
#'-------------------------------------------------------------------

process_sinan_tb <- function(
    anos,
    directory,
    municipality_data = TRUE,
    paisnet_csv = NULL,     # <-- opcional: caminho do CSV com ID_PAIS / NM_PAIS
    cbo_csv     = NULL,     # <-- opcional: caminho do CSV com cod / nome (CBO)
    quiet = FALSE
) {
  # Dependências mínimas
  pkgs_needed <- c("dplyr","forcats","stringi","rlang","tibble")
  miss <- pkgs_needed[!vapply(pkgs_needed, requireNamespace, FUN.VALUE = logical(1), quietly = TRUE)]
  if (length(miss)) stop("Faltam pacotes: ", paste(miss, collapse=", "), ".")

  # ---- Carrega lookups a partir de CSV ou microdatasus (se disponível) ----
  paisnet_df <- NULL
  if (!is.null(paisnet_csv) && file.exists(paisnet_csv)) {
    paisnet_df <- utils::read.csv(paisnet_csv, stringsAsFactors = FALSE, encoding = "UTF-8")
    names(paisnet_df) <- toupper(names(paisnet_df))
    req <- c("ID_PAIS","NM_PAIS")
    if (!all(req %in% names(paisnet_df))) {
      if (!quiet) message("Aviso: paisnet_csv não tem colunas esperadas (ID_PAIS, NM_PAIS). Ignorando.")
      paisnet_df <- NULL
    } else {
      paisnet_df <- paisnet_df[, req]
    }
  } else if (requireNamespace("microdatasus", quietly = TRUE)) {
    paisnet_df <- microdatasus::paisnet
    names(paisnet_df) <- toupper(names(paisnet_df))
    paisnet_df <- paisnet_df[, intersect(c("ID_PAIS","NM_PAIS"), names(paisnet_df))]
  } else {
    if (!quiet) message("Sem paisnet (nem CSV nem microdatasus). País não será rotulado.")
  }

  cbo_df <- NULL
  if (!is.null(cbo_csv) && file.exists(cbo_csv)) {
    cbo_df <- utils::read.csv(cbo_csv, stringsAsFactors = FALSE, encoding = "UTF-8")
    names(cbo_df) <- tolower(names(cbo_df))
    req <- c("cod","nome")
    if (!all(req %in% names(cbo_df))) {
      if (!quiet) message("Aviso: cbo_csv não tem colunas esperadas (cod, nome). Ignorando.")
      cbo_df <- NULL
    } else {
      cbo_df <- cbo_df[, req]
    }
  } else if (requireNamespace("microdatasus", quietly = TRUE)) {
    cbo_df <- microdatasus::tabOcupacao
    names(cbo_df) <- tolower(names(cbo_df))
    cbo_df <- cbo_df[, intersect(c("cod","nome"), names(cbo_df))]
  } else {
    if (!quiet) message("Sem tabOcupacao (nem CSV nem microdatasus). Ocupação não será rotulada.")
  }
  if (!is.null(cbo_df) && "cod" %in% names(cbo_df)) cbo_df$cod <- as.character(cbo_df$cod)

  # ---- Mapas estáveis (UF, região, capitais, recodes) ----
  uf_names <- c(
    "0"="Ignorado","99"="Ignorado","/N"="Ignorado",
    "11"="Rondônia","12"="Acre","13"="Amazonas","14"="Roraima","15"="Pará","16"="Amapá","17"="Tocantins",
    "21"="Maranhão","22"="Piauí","23"="Ceará","24"="Rio Grande do Norte","25"="Paraíba","26"="Pernambuco",
    "27"="Alagoas","28"="Sergipe","29"="Bahia",
    "31"="Minas Gerais","32"="Espírito Santo","33"="Rio de Janeiro","35"="São Paulo",
    "41"="Paraná","42"="Santa Catarina","43"="Rio Grande do Sul",
    "50"="Mato Grosso do Sul","51"="Mato Grosso","52"="Goiás","53"="Distrito Federal"
  )
  region_map <- c(
    "0"="Ignorado","99"="Ignorado","/N"="Ignorado",
    "11"="Norte","12"="Norte","13"="Norte","14"="Norte","15"="Norte","16"="Norte","17"="Norte",
    "21"="Nordeste","22"="Nordeste","23"="Nordeste","24"="Nordeste","25"="Nordeste",
    "26"="Nordeste","27"="Nordeste","28"="Nordeste","29"="Nordeste",
    "31"="Sudeste","32"="Sudeste","33"="Sudeste","35"="Sudeste",
    "41"="Sul","42"="Sul","43"="Sul",
    "50"="Centro-Oeste","51"="Centro-Oeste","52"="Centro-Oeste","53"="Centro-Oeste"
  )
  capital_map <- c(
    "280030"="Aracaju","150140"="Belém","310620"="Belo Horizonte","140010"="Boa Vista",
    "530010"="Brasília","500270"="Campo Grande","510340"="Cuiabá","410690"="Curitiba",
    "420540"="Florianópolis","230440"="Fortaleza","520870"="Goiânia","250750"="João Pessoa",
    "160030"="Macapá","270430"="Maceió","130260"="Manaus","240810"="Natal","172100"="Palmas",
    "431490"="Porto Alegre","110020"="Porto Velho","261160"="Recife","120040"="Rio Branco",
    "330455"="Rio de Janeiro","292740"="Salvador","211130"="São Luís","355030"="São Paulo",
    "221100"="Teresina","320530"="Vitória"
  )
  simple_maps <- list(
    TP_NOT       = c("1"="Negativa","2"="Individual","3"="Surto","4"="Agregado"),
    CS_SEXO      = c("M"="Masculino","F"="Feminino","I"="Ignorado"),
    CS_GESTANT   = c("1"="1ºTrimestre","2"="2ºTrimestre","3"="3ºTrimestre",
                     "4"="Idade gestacional Ignorada","5"="Não","6"="Não se aplica","9"="Ignorado"),
    CS_RACA      = c("0"="Ignorado","1"="Branca","2"="Preta","3"="Amarela","4"="Parda","5"="Indígena","6"="Ignorado","9"="Ignorado"),
    CS_ESCOL_N   = c("0"="Analfabeto","1"="1a a 4a série incompleta do EF","2"="4a série completa do EF","3"="5a a 8a série incompleta do EF",
                     "4"="Ensino fundamental completo","5"="Ensino médio incompleto","6"="Ensino médio completo","7"="Educação superior incompleta",
                     "8"="Educação superior completa","9"="Ignorado","10"="Não se aplica"),
    TRATAMENTO   = c("0"="Ignorado","1"="Caso Novo","2"="Recidiva","3"="Reingresso após abandono","4"="Não sabe","5"="Transferência","6"="Pós-óbito"),
    INSTITUCIO   = c("0"="Ignorado","1"="Não","2"="Presídio","3"="Asilo","4"="Orfanato","5"="Hospital psiquiátrico","6"="Outro","9"="Ignorado"),
    RAIOX_TORA   = c("0"="Ignorado","1"="Suspeito","2"="Normal","3"="Outra patologia","4"="Não realizado","9"="Ignorado"),
    FORMA        = c("0"="Ignorado","1"="Pulmonar","2"="Extrapulmonar","3"="Pulmonar + Extrapulmonar"),
    EXTRAPU1_N   = c("0"="Ignorado","1"="Pleural","2"="Ganglionar periférico","3"="Geniturinário","4"="Ósseo","5"="Ocular","6"="Miliar","7"="Menigocefálico","8"="Cutâneo","9"="Laringea","10"="Outro","99"="Ignorado"),
    EXTRAPU2_N   = c("0"="Ignorado","1"="Pleural","2"="Ganglionar periférico","3"="Geniturinário","4"="Ósseo","5"="Ocular","6"="Miliar","7"="Menigocefálico","8"="Cutâneo","9"="Laringea","10"="Outro","99"="Ignorado"),
    POP_LIBER    = c("0"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    POP_RUA      = c("0"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    POP_SAUDE    = c("0"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    POP_IMIG     = c("0"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    BENEF_GOV    = c("0"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    AGRAVAIDS    = c("0"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    AGRAVALCOO   = c("0"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    AGRAVDIABE   = c("0"="Ignorado","1"="Sim","2"="Não","3"="Ignorado","9"="Ignorado"),
    AGRAVDOENC   = c("0"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    AGRAVDROGA   = c("0"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    AGRAVTABAC   = c("0"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    AGRAVOUTRA   = c("0"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    BACILOSC_E   = c("0"="Ignorado","1"="Positiva","2"="Negativa","3"="Não realizada","4"="Não se aplica","9"="Ignorado"),
    BACILOS_E2   = c("0"="Ignorado","1"="Positiva","2"="Negativa","3"="Não realizada","4"="Não se aplica","9"="Ignorado"),
    BACILOSC_O   = c("0"="Ignorado","1"="Positiva","2"="Negativa","3"="Não realizada","4"="Não se aplica","9"="Ignorado"),
    CULTURA_ES   = c("0"="Ignorado","1"="Positiva","2"="Negativa","3"="Em andamento","4"="Não realizado","9"="Ignorado"),
    CULTURA_OU   = c("0"="Ignorado","1"="Positiva","2"="Negativa","3"="Em andamento","4"="Não realizado","9"="Ignorado"),
    HIV          = c("0"="Ignorado","1"="Positivo","2"="Negativo","3"="Em andamento","4"="Não realizado","9"="Ignorado"),
    HISTOPATOL   = c("0"="Ignorado","1"="BAAR positivo","2"="Sugestivo de TB","3"="Não sugestivo de TB","4"="Em andamento","5"="Não realizado","9"="Ignorado"),
    RIFAMPICIN   = c("/"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    ISONIAZIDA   = c("/"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    ETAMBUTOL    = c("/"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    ESTREPTOMI   = c("/"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    PIRAZINAMI   = c("/"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    ETIONAMIDA   = c("/"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    OUTRAS       = c("/"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    TRAT_SUPER   = c("0"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    TRATSUP_AT   = c("0"="Ignorado","1"="Sim","2"="Não","4"="Ignorado","9"="Ignorado"),
    DOENCA_TRA   = c("/"="Ignorado","1"="Sim","2"="Não","9"="Ignorado"),
    BACILOSC_1   = c("0"="Ignorado","1"="Positiva","2"="Negativa","3"="Não realizada","4"="Não se aplica","9"="Ignorado"),
    BACILOSC_2   = c("0"="Ignorado","1"="Positiva","2"="Negativa","3"="Não realizada","4"="Não se aplica","9"="Ignorado"),
    BACILOSC_3   = c("0"="Ignorado","1"="Positiva","2"="Negativa","3"="Não realizada","4"="Não se aplica","9"="Ignorado"),
    BACILOSC_4   = c("0"="Ignorado","1"="Positiva","2"="Negativa","3"="Não realizada","4"="Não se aplica","9"="Ignorado"),
    BACILOSC_5   = c("0"="Ignorado","1"="Positiva","2"="Negativa","3"="Não realizada","4"="Não se aplica","9"="Ignorado"),
    BACILOSC_6   = c("0"="Ignorado","1"="Positiva","2"="Negativa","3"="Não realizada","4"="Não se aplica","9"="Ignorado"),
    BAC_APOS_6   = c("0"="Ignorado","1"="Positiva","2"="Negativa","3"="Não realizada","4"="Não se aplica","9"="Ignorado"),
    TEST_MOLEC   = c("1"="Detectável sensível à Rifampicina","2"="Detectável resistente à Rifampicina","3"="Não detectável","4"="Inconclusivo","5"="Não realizado"),
    TEST_SENSI   = c("1"="Resistente somente à Isoniazida","2"="Resistente somente à Rifampicina","3"="Resistente à Isoniazida e Rifampicina","4"="Resistente a outras drogas de 1ª linha","5"="Sensível","6"="Em andamento","7"="Não realizado","8"="Ignorado","9"="Ignorado", ","="Ignorado"),
    ANT_RETRO    = c("0"="Ignorado","1"="Sim","2"="Não","3"="Ignorado","4"="Ignorado","5"="Ignorado","6"="Ignorado","7"="Ignorado","8"="Ignorado","9"="Ignorado","S"="Ignorado"),
    TRANSF       = c("1"="Mesmo município","2"="Município diferente (mesma UF)","3"="UF diferente","4"="País diferente","9"="Ignorado","0"="Ignorado"),
    SITUA_9_M    = c("0"="Ignorado","1"="Cura","2"="Abandono","3"="Óbito por TB","4"="Óbito por outra causa","5"="Transferência para mesmo município (outra unidade)","6"="Transferência para outro município (mesma UF)","7"="Transferência para outro Estado","8"="Transferência para outro país","9"="Mudança de esquema por intolerância medicamentosa","10"="Mudança de diagnóstico","11"="Falência","12"="Continua em tratamento","13"="TBMultiresistente"),
    SITUA_12_M   = c("0"="Ignorado","1"="Cura","2"="Abandono","3"="Óbito por TB","4"="Óbito por outra causa","5"="Transferência para mesmo município (outra unidade)","6"="Transferência para outro município (mesma UF)","7"="Transferência para outro Estado","8"="Transferência para outro país","9"="Mudança de esquema por intolerância medicamentosa","10"="Mudança de diagnóstico","11"="Continua em tratamento","12"="Continua em tratamento"),
    SITUA_ENCE   = c("-1"="Cura","0"="Ignorado","03"="Óbito por TB","04"="Óbito por outra causa","1"="Cura","2"="Abandono","3"="Óbito por TB","4"="Óbito por outra causa","5"="Transferência","6"="Mudança de diagnóstico","7"="TB-DR","8"="Mudança de esquema","9"="Falência","10"="Abandono primário","99"="Ignorado")
  )
  label_to_code <- stats::setNames(names(uf_names), unname(uf_names))

  # --- Helpers (sem usar !!!) ---
  relevel_to <- function(x, lev, na_as = "Ignorado", ordered = FALSE) {
    x <- base::as.factor(x)
    x <- forcats::fct_na_value_to_level(x, level = na_as)
    x <- forcats::fct_expand(x, lev)
    do.call(forcats::fct_relevel, c(list(x), as.list(lev)))
  }

  # ---- Processador interno ----
  processar_sinan_tb_inner <- function(data) {
    data <- tibble::as_tibble(data)
    
    # Númericas
    num_vars <- intersect(c("NU_ANO","ID_MUNICIP","ID_REGIONA","ANO_NASC",
                            "ID_MN_RESI","ID_RG_RESI","NU_CONTATO","NU_COMU_EX",
                            "UF_TRANSF", "MUN_TRANSF"), names(data))
    if (length(num_vars)) {
      data <- dplyr::mutate(
        data,
        dplyr::across(
          dplyr::all_of(num_vars),
          ~ suppressWarnings(as.numeric(as.character(.x)))))
    }
    
    # Datas
    date_vars <- intersect(c("DT_NOTIFIC","DT_DIAG","DT_DIGITA","DT_TRANSUS","DT_TRANSDM","DT_TRANSSM","DT_TRANSRM","DT_TRANSRS","DT_TRANSSE","DT_INIC_TR","DT_NOTI_AT","DT_MUDANCA","DT_ENCERRA"), names(data))
    if (length(date_vars) > 0) {
      data <- dplyr::mutate(data, dplyr::across(dplyr::all_of(date_vars), ~ as.Date(.)))
    }

    # Recodes simples (sempre factor)
    for (var in intersect(names(simple_maps), names(data))) {
      mapa <- simple_maps[[var]]
      data <- dplyr::mutate(
        data,
        !!var := as.character(.data[[var]]),
        !!var := dplyr::recode(!!rlang::sym(var), !!!as.list(mapa), .default = NA_character_, .missing = "Ignorado"),
        !!var := base::factor(.data[[var]], exclude = NULL)
      )
    }

    # UF/Região – residência 
    if ("SG_UF" %in% names(data)) {
      sguf_code <- dplyr::recode(as.character(data$SG_UF), !!!as.list(label_to_code), .default = as.character(data$SG_UF), .missing = NA_character_)
      data <- dplyr::mutate(
        data,
        REGIAO_RES = dplyr::recode(sguf_code, !!!as.list(region_map), .default = NA_character_, .missing = "Ignorado"),
        REGIAO_RES = base::factor(REGIAO_RES, levels = c("Norte","Nordeste","Sudeste","Sul","Centro-Oeste","Ignorado")),
        UF_RES     = suppressWarnings(as.numeric(as.character(sguf_code))), 
        SG_UF      = dplyr::recode(sguf_code, !!!as.list(uf_names), .default = NA_character_, .missing = "Ignorado"),
        SG_UF      = base::factor(SG_UF)
      )
    }

    # UF/Região – notificação
    if ("SG_UF_NOT" %in% names(data)) {
      sguf_not_code <- dplyr::recode(as.character(data$SG_UF_NOT), !!!as.list(label_to_code), .default = as.character(data$SG_UF_NOT), .missing = NA_character_)
      data <- dplyr::mutate(
        data,
        REGIAO_NOT = dplyr::recode(sguf_not_code, !!!as.list(region_map), .default = NA_character_, .missing = "Ignorado"),
        REGIAO_NOT = base::factor(REGIAO_NOT, levels = c("Norte","Nordeste","Sudeste","Sul","Centro-Oeste","Ignorado")),
        UF_NOT     = suppressWarnings(as.numeric(as.character(sguf_not_code))), 
        SG_UF_NOT  = dplyr::recode(sguf_not_code, !!!as.list(uf_names), .default = NA_character_, .missing = "Ignorado"),
        SG_UF_NOT  = base::factor(SG_UF_NOT)
      )
    }

    # Capitais
    if (isTRUE(municipality_data) && "ID_MN_RESI" %in% names(data)) {
      data <- dplyr::mutate(
        data,
        CAPITAL_RES = dplyr::recode(as.character(ID_MN_RESI), !!!as.list(capital_map), .default = NA_character_),
        CAPITAL_RES = base::factor(CAPITAL_RES)
      )
    }
    muni_not_col <- intersect(c("ID_MUNICIP","ID_MN_NOT"), names(data))[1]
    if (isTRUE(municipality_data) && !is.na(muni_not_col)) {
      v <- muni_not_col
      data <- dplyr::mutate(
        data,
        CAPITAL_NOT = dplyr::recode(as.character(.data[[v]]), !!!as.list(capital_map), .default = NA_character_),
        CAPITAL_NOT = base::factor(CAPITAL_NOT)
      )
    }

    # Idade / Faixa etária
    if ("NU_IDADE_N" %in% names(data)) {
      faixa_lvls <- c("0 a 4 anos","5 a 9 anos","10 a 14 anos","15 a 19 anos","20 a 24 anos",
                      "25 a 29 anos","30 a 34 anos","35 a 39 anos","40 a 44 anos","45 a 49 anos",
                      "50 a 54 anos","55 a 59 anos","60 a 64 anos","65 a 69 anos","70 a 74 anos",
                      "75 a 79 anos","80 anos e mais","Ignorado")
      data <- dplyr::mutate(
        data,
        NU_IDADE_N  = dplyr::na_if(as.character(NU_IDADE_N), "000"),
        NU_IDADE_N  = dplyr::na_if(NU_IDADE_N, "999"),
        .cod        = substr(NU_IDADE_N, 1, 2),
        .val        = suppressWarnings(as.numeric(substr(NU_IDADE_N, 3, 5))),
        IDADE       = dplyr::case_when(
          .cod == "00" ~ .val,
          .cod %in% c("10","20","30") ~ 0,
          .cod %in% c("40","41") ~ .val,
          TRUE ~ NA_real_
        ),
        FAIXA_ETARIA = dplyr::case_when(
          IDADE >= 0  & IDADE <= 4  ~ "0 a 4 anos",
          IDADE >= 5  & IDADE <= 9  ~ "5 a 9 anos",
          IDADE >= 10 & IDADE <= 14 ~ "10 a 14 anos",
          IDADE >= 15 & IDADE <= 19 ~ "15 a 19 anos",
          IDADE >= 20 & IDADE <= 24 ~ "20 a 24 anos",
          IDADE >= 25 & IDADE <= 29 ~ "25 a 29 anos",
          IDADE >= 30 & IDADE <= 34 ~ "30 a 34 anos",
          IDADE >= 35 & IDADE <= 39 ~ "35 a 39 anos",
          IDADE >= 40 & IDADE <= 44 ~ "40 a 44 anos",
          IDADE >= 45 & IDADE <= 49 ~ "45 a 49 anos",
          IDADE >= 50 & IDADE <= 54 ~ "50 a 54 anos",
          IDADE >= 55 & IDADE <= 59 ~ "55 a 59 anos",
          IDADE >= 60 & IDADE <= 64 ~ "60 a 64 anos",
          IDADE >= 65 & IDADE <= 69 ~ "65 a 69 anos",
          IDADE >= 70 & IDADE <= 74 ~ "70 a 74 anos",
          IDADE >= 75 & IDADE <= 79 ~ "75 a 79 anos",
          IDADE >= 80               ~ "80 anos e mais",
          TRUE ~ "Ignorado"
        ),
        FAIXA_ETARIA = base::factor(FAIXA_ETARIA, levels = faixa_lvls, ordered = TRUE)
      )
      data$.cod <- NULL; data$.val <- NULL
    }

    # Critério laboratorial
    if (all(c("BACILOSC_E","BACILOS_E2","CULTURA_ES","TEST_MOLEC") %in% names(data))) {
      data <- dplyr::mutate(
        data,
        CRITERIO_LABORATORIAL = dplyr::case_when(
          BACILOSC_E == "Positiva" |
            BACILOS_E2 == "Positiva" |
            CULTURA_ES == "Positiva" |
            TEST_MOLEC %in% c("Detectável sensível à Rifampicina","Detectável resistente à Rifampicina")
          ~ "Com critério laboratorial",
          TRUE ~ "Sem critério laboratorial"
        ),
        CRITERIO_LABORATORIAL = base::factor(CRITERIO_LABORATORIAL, levels = c("Com critério laboratorial","Sem critério laboratorial"))
      )
    }

    # País (via CSV ou micro)
    if (!is.null(paisnet_df) && "ID_PAIS" %in% names(data)) {
      data$ID_PAIS <- as.character(data$ID_PAIS)
      data <- dplyr::left_join(data, paisnet_df, by = "ID_PAIS")
      if ("NM_PAIS" %in% names(data)) {
        data$ID_PAIS <- base::factor(data$NM_PAIS)
        data <- dplyr::select(data, -NM_PAIS)
      }
    }

    # Ocupação (via CSV ou micro)
    if (!is.null(cbo_df) && "ID_OCUPA_N" %in% names(data)) {
      data$ID_OCUPA_N <- as.character(data$ID_OCUPA_N)
      data <- dplyr::left_join(data, cbo_df, by = c("ID_OCUPA_N" = "cod"))
      if ("nome" %in% names(data)) {
        data$ID_OCUPA_N <- base::factor(data$nome)
        data <- dplyr::select(data, -nome)
      }
    }

    # Reordenações padronizadas (Y/N)
    yn_vars <- intersect(c("POP_LIBER","POP_RUA","POP_SAUDE","POP_IMIG","BENEF_GOV",
                           "AGRAVAIDS","AGRAVALCOO","AGRAVDIABE","AGRAVDOENC",
                           "AGRAVDROGA","AGRAVTABAC","AGRAVOUTRA",
                           "TRAT_SUPER","TRATSUP_AT","DOENCA_TR","DOENCA_TRA",
                           "RIFAMPICIN","ISONIAZIDA","ETAMBUTOL","ESTREPTOMI","PIRAZINAMI","ETIONAMIDA","OUTRAS"),
                         names(data))
    if (length(yn_vars) > 0) {
      yn_lvls <- c("Sim","Não","Ignorado")
      for (v in yn_vars) data[[v]] <- relevel_to(data[[v]], yn_lvls)
    }

    # Baciloscopia / Cultura / HIV / etc (níveis fixos)
    bac_vars <- intersect(c("BACILOSC_E","BACILOS_E2","BACILOSC_O","BACILOSC_1","BACILOSC_2","BACILOSC_3","BACILOSC_4","BACILOSC_5","BACILOSC_6","BAC_APOS_6"), names(data))
    if (length(bac_vars) > 0) {
      lev_bac <- c("Positiva","Negativa","Não realizada","Não se aplica","Ignorado")
      for (v in bac_vars) data[[v]] <- relevel_to(data[[v]], lev_bac)
    }

    cult_vars <- intersect(c("CULTURA_ES","CULTURA_OU"), names(data))
    if (length(cult_vars) > 0) {
      lev_cult <- c("Positiva","Negativa","Em andamento","Não realizado","Ignorado")
      for (v in cult_vars) data[[v]] <- relevel_to(data[[v]], lev_cult)
    }

    if ("HIV" %in% names(data)) {
      data$HIV <- relevel_to(data$HIV, c("Positivo","Negativo","Em andamento","Não realizado","Ignorado"))
    }
    if ("HISTOPATOL" %in% names(data)) {
      data$HISTOPATOL <- relevel_to(data$HISTOPATOL, c("BAAR positivo","Sugestivo de TB","Não sugestivo de TB","Em andamento","Não realizado","Ignorado"))
    }
    if ("TEST_MOLEC" %in% names(data)) {
      data$TEST_MOLEC <- relevel_to(data$TEST_MOLEC, c("Detectável sensível à Rifampicina","Detectável resistente à Rifampicina","Não detectável","Inconclusivo","Não realizado"))
    }
    if ("TEST_SENSI" %in% names(data)) {
      data$TEST_SENSI <- relevel_to(data$TEST_SENSI, c("Resistente somente à Isoniazida","Resistente somente à Rifampicina","Resistente à Isoniazida e Rifampicina","Resistente a outras drogas de 1ª linha","Sensível","Em andamento","Não realizado","Ignorado"))
    }
    if ("TRANSF" %in% names(data)) {
      data$TRANSF <- relevel_to(data$TRANSF, c("Mesmo município","Município diferente (mesma UF)","UF diferente","País diferente","Ignorado"))
    }

    # SITUA_9_M e SITUA_12_M (sempre com "Ignorado")
    if ("SITUA_9_M" %in% names(data)) {
      data$SITUA_9_M <- relevel_to(
        dplyr::recode(as.character(data$SITUA_9_M), !!!as.list(simple_maps$SITUA_9_M), .default = NA_character_, .missing = "Ignorado"),
        c("Cura","Abandono","Óbito por TB","Óbito por outra causa",
          "Transferência para mesmo município (outra unidade)",
          "Transferência para outro município (mesma UF)",
          "Transferência para outro Estado","Transferência para outro país",
          "Mudança de esquema por intolerância medicamentosa",
          "Mudança de diagnóstico","Falência","Continua em tratamento",
          "TBMultiresistente","Ignorado")
      )
    }
    if ("SITUA_12_M" %in% names(data)) {
      data$SITUA_12_M <- relevel_to(
        dplyr::recode(as.character(data$SITUA_12_M), !!!as.list(simple_maps$SITUA_12_M), .default = NA_character_, .missing = "Ignorado"),
        c("Cura","Abandono","Óbito por TB","Óbito por outra causa",
          "Transferência para mesmo município (outra unidade)",
          "Transferência para outro município (mesma UF)",
          "Transferência para outro Estado","Transferência para outro país",
          "Mudança de esquema por intolerância medicamentosa",
          "Mudança de diagnóstico","Continua em tratamento","Ignorado")
      )
    }

    # Limpeza final de strings
    data <- dplyr::mutate(data, dplyr::across(dplyr::where(is.character), stringi::stri_unescape_unicode))
    data
  }

  # ---- Leitura + processamento por ano (auto: CSV primeiro, depois DBC) ----
  out <- vector("list", length(anos))
  names(out) <- as.character(anos)

  for (i in seq_along(anos)) {
    ano <- anos[i]
    base <- paste0("TUBEBR", substr(as.character(ano), 3, 4))
    caminho_dbc <- file.path(directory, paste0(base, ".dbc"))
    caminho_csv <- file.path(directory, paste0(base, ".csv"))

    tem_dbc <- file.exists(caminho_dbc)
    tem_csv <- file.exists(caminho_csv)

    if (!tem_dbc && !tem_csv) {
      if (!quiet) message("Arquivo não encontrado (csv/dbc): ", base)
      next
    }

    if (!quiet) message("Processando arquivo: ", if (tem_dbc) basename(caminho_dbc) else basename(caminho_csv))

    # leitura
    if (tem_dbc) {
      # DBC requer o pacote read.dbc
      if (!requireNamespace("read.dbc", quietly = TRUE)) {
        message("Pacote 'read.dbc' não instalado e apenas DBC disponível: ", basename(caminho_dbc))
        next
      }
      banco <- tryCatch(
        read.dbc::read.dbc(caminho_dbc),
        error = function(e) {
          message("Erro ao ler DBC: ", basename(caminho_dbc), " -> ", e$message)
          NULL
        }
      )
    } else if (tem_csv) {
      # CSV exportado do SINAN/DBF: separador vírgula e UTF-8
      banco <- tryCatch(
        utils::read.csv(caminho_csv, stringsAsFactors = FALSE, encoding = "UTF-8", na.strings = c("", "NA")),
        error = function(e) {
          message("Erro ao ler CSV: ", basename(caminho_csv), " -> ", e$message)
          NULL
        }
      )
    } else {
      message("Nenhum arquivo CSV ou DBC encontrado para: ", base)
      next
    }
    if (is.null(banco)) next

    # processa + ANO
    banco_proc <- tryCatch({
      res <- processar_sinan_tb_inner(banco)
      dplyr::mutate(res, ANO = ano)
    }, error = function(e) {
      message("Erro ao processar: ", base, " -> ", e$message)
      NULL
    })

    out[[i]] <- banco_proc
  }

  out <- Filter(Negate(is.null), out)
  if (!length(out)) tibble::tibble() else dplyr::bind_rows(out)
}
