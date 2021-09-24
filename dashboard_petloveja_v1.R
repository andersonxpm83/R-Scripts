rm(list=ls())

source("~/R/Acessos.R")


### Queries

# Base consolidada BH + Mini HUB SP

sqlQuery_base_minihub_proprio <-
  "with
    pedidos_petloveja as
    (
    select distinct 
    fpf.pedido_beagle as id_pedido
    from dw_corporativo.ft_pedido_faturado fpf
    left join dw_corporativo.dim_filial df on df.chv_filial = fpf.chv_filial
    where 
    df.nm_filial in ('CD Minihub São Paulo',
                     'CD Minihub Porto Alegre',
                     'CD Minihub Brasília')
    and
    fpf.chv_data_emissao_nota_fiscal >= '2020-01-01'
    )
    select
        df.nm_filial as unidade_petloveja
        , fpf.chv_data_emissao_nota_fiscal as data_pedido
        , cast(fpf.pedido_beagle as numeric) as id_pedido
        , (case
           when df.nm_filial = 'CD Minihub São Paulo'
        	 then 'sim'
        	 when df.nm_filial = 'CD Minihub Porto Alegre'
        	 then 'sim'
        	 when df.nm_filial = 'CD Minihub Brasília'
        	 then 'sim'
        	 else 'nao' end) as entrega_petloveja
        , fpf.chv_cliente
        , fpf.flg_primeira_compra as cliente_novo
        , dm.nm_municipio
        , fpf.cep_entrega
        , dcv.canal_venda 
        , dp.erp_setor
        , dp.erp_familia
        , dp.erp_subfamilia
        , dp.sku
        , dp.nome as nm_sku
        , 'metodos_de_pagamento' as metodos_de_pagamento
        , fpf.quantidade
        , fpf.valor_mercadoria
        , fpf.custo_produto
        , fpf.receita_bruta_total
        , fpf.desconto_condicional
        , fpf.desconto_incondicional
        , fpf.desconto
        , (fpf.receita_bruta_total-fpf.desconto_incondicional) as receita_desconto_carrinho
        , fpf.receita_bruta_frete as receita_bruta_frete
        , TO_CHAR(CURRENT_DATE-1,'YYYY-MM-DD') as prefdate
        from dw_corporativo.ft_pedido_faturado fpf 
        left join dw_corporativo.dim_canal_venda dcv on dcv.chv_canal_venda = fpf.chv_canal_venda 
        left join dw_corporativo.dim_cliente dc on dc.chv_cliente = fpf.chv_cliente
        left join dw_corporativo.dim_filial df on df.chv_filial = fpf.chv_filial 
        left join dw_corporativo.dim_municipio dm on dm.chv_municipio = fpf.chv_municipio_entrega 
        left join dw_corporativo.dim_produto dp on dp.chv_produto = fpf.chv_produto
        inner join pedidos_petloveja ppj on ppj.id_pedido = fpf.pedido_beagle
        where
        fpf.chv_data_emissao_nota_fiscal >= '2020-01-01'"

sqlQuery_base_minihub_parceiro <-
  "select
    case
    when pos3.cidade in (' Belo Horizonte', 
    					 'Araguari',
    					 'BELO HORIZONTE',
    					 'BH',
    					 'Belo', 
    					 'Belo Horizonte',
    					 'Belo horizonte',
    					 'Belo hosrizonte',
    					 'BeloHorizonte', 
    					 'Betim',
    					 'Contagem',
    					 'Ibirité',
    					 'Igarapé',
    					 'Lagoa Santa',
    					 'Nova Lima',
    					 'Ribeirão das Neves',
    					 'Santa Luzia',
    					 'Vespasiano',
    					 'belo horizonte', 
    					 'bh',
    					 'contagem')
    					 then 'CD Minihub Belo Horizonte'
    when pos3.cidade in ('Santo André',
                         'São Bernardo do Campo')
                         then 'CD Minihub ABC'
    when pos3.cidade in ('Porto Alegre')
                         then 'CD Minihub Porto Alegre'
    when pos3.cidade in ('Curitiba',
                         'Carambeí')
                         then 'CD Minihub Curitiba'
                         else 'CD Minihub X'
                         end as unidade_petloveja
    , to_char(pos3.order_date, 'yyyy-mm-dd') as data_pedido
    , cast(pos3.id_pedido as numeric) as id_pedido
    --, 'CD Minihub Belo Horizonte' as nm_filial
    , pos3.entrega_omni as entrega_petloveja
    , cast(pos3.user_id as numeric) as chv_cliente
    , pos3.new_client as cliente_novo
    , pos3.cidade as nm_municipio
    , cast(pos3.cep as numeric) as cep_entrega
    , pos3.canal as canal_venda
    , pos3.setor as erp_setor
    , pos3.familia as erp_familia
    , pos3.subfamilia as erp_subfamilia
    , pos3.sku
    , pos3.item_nome as nm_sku
    , pos3.metodos_de_pagamento
    , pos3.quantidade_item as quantidade
    , pos3.valor_mercadoria
    , pos3.preco_item as custo_produto
    , pos3.receita_bruta_produto_total as receita_bruta_total
    , pos3.descontos_credito_carteira as desconto_condicional
    , pos3.descontos_de_carrinho as desconto_incondicional
    , pos3.descontos_totais as desconto
    , pos3.receita_produto_desconto_carrinho as receita_desconto_carrinho
    , pos3.receita_frete_rateada as receita_bruta_frete
    , to_char(current_date-1,'YYYY-MM-DD') as prefdate
    from analytics_base.pnl_omni pos3
    where date(pos3.order_date) >= '2020-01-01'"

sqlQuery_pedidos_brinde <-
  "select distinct 
    fpf.pedido_beagle as id_pedido
    from dw_corporativo.ft_pedido_faturado fpf
    where
    fpf.tipo_faturamento_item = 'brinde'
    and
    fpf.chv_data_emissao_nota_fiscal >= '2020-01-01'"

sqlQuery_base_spot <-
  "select 
    case
    when dm.nm_municipio in ('Belo Horizonte',
                             'Contagem',
                             'Nova Lima',
                             'Betim',
                             'Santa Luzia',
                             'Ribeirão das Neves',
                             'Lagoa Santa',
                             'Sabará',
                             'Vespasiano',
                             'Ibirité',
                             'Pedro Leopoldo',
                             'Caeté',
                             'Igarapé',
                             'São José da Lapa',
                             'Sarzedo',
                             'Rio Acima',
                             'Esmeraldas',
                             'Mário Campos',
                             'Nova União',
                             'Confins',
                             'Raposos',
                             'São Joaquim de Bicas',
                             'Taquaraçu de Minas',
                             'Uberlândia',
                             'Rio Pomba',
                             'Montes Claros')
    					     then 'CD Minihub Belo Horizonte'
    when dm.nm_municipio in ('Santo André',
                             'São Bernardo do Campo',
                             'São Caetano do Sul',
                             'Diadema',
                             'Mauá')
                             then 'CD Minihub ABC'
    when dm.nm_municipio in ('Porto Alegre',
                             'Agudo',
                             'Independência')
                             then 'CD Minihub Porto Alegre'
    when dm.nm_municipio in ('São Paulo',
    				                 'Guarulhos',
    				                 'Osasco',
    				                 'Barueri',
    				                 'Santana de Parnaíba',
    				                 'Taboão da Serra',
    				                 'Cotia',
    				                 'Carapicuíba',
    				                 'Embu das Artes',
    				                 'São José dos Campos',
    				                 'Atibaia',
    				                 'Taquarituba',
    				                 'Jandira',
    				                 'Praia Grande',
    				                 'Junqueirópolis',
    				                 'Campinas')
                             then 'CD Minihub São Paulo'
                             else 'CD Minihub X'
                             end as unidade_petloveja
    , to_char(fpf.chv_data_emissao_nota_fiscal, 'yyyymm') as ano_mes_pedidos
    , fpf.chv_data_emissao_nota_fiscal as dia_pedidos
    , count(distinct fpf.pedido_beagle) as qtd_pedidos_spot
    from dw_corporativo.ft_pedido_faturado fpf
    left join dw_corporativo.dim_canal_venda dcv on dcv.chv_canal_venda = fpf.chv_canal_venda 
    left join dw_corporativo.dim_cliente dc on dc.chv_cliente = fpf.chv_cliente
    left join dw_corporativo.dim_filial df on df.chv_filial = fpf.chv_filial
    left join dw_corporativo.dim_municipio dm on dm.chv_municipio = fpf.chv_municipio_entrega 
    left join dw_corporativo.dim_produto dp on dp.chv_produto = fpf.chv_produto
    where 
    fpf.elegivel_petloveja = 'Sim'
    and
      (case when dcv.subcanal_venda is null
            then 'VAZIO' else dcv.subcanal_venda end) != 'AGENDADO'
    and 
    fpf.chv_data_emissao_nota_fiscal >= '2020-01-01'
    group by 
    1, 2, 3"

sqlQuery_base_prazo_entrega <-
  "select distinct
    case
    when pos3.cidade in (' Belo Horizonte', 
    					 'Araguari',
    					 'BELO HORIZONTE',
    					 'BH',
    					 'Belo', 
    					 'Belo Horizonte',
    					 'Belo horizonte',
    					 'Belo hosrizonte',
    					 'BeloHorizonte', 
    					 'Betim',
    					 'Contagem',
    					 'Ibirité',
    					 'Igarapé',
    					 'Lagoa Santa',
    					 'Nova Lima',
    					 'Ribeirão das Neves',
    					 'Santa Luzia',
    					 'Vespasiano',
    					 'belo horizonte', 
    					 'bh',
    					 'contagem')
    					 then 'CD Minihub Belo Horizonte'
    when pos3.cidade in ('Santo André',
                         'São Bernardo do Campo')
                         then 'CD Minihub ABC'
    when pos3.cidade in ('Porto Alegre')
                         then 'CD Minihub Porto Alegre'
    when pos3.cidade in ('Curitiba',
                         'Carambeí')
                         then 'CD Minihub Curitiba'
                         else 'CD Minihub X'
                         end as nm_filial
    , pos3.id_pedido as pedido_beagle
    , pos3.order_date as data_pedido
    , pos3.order_date as data_pedido_aprovacao
    , fe.chv_entrega
    , fe.data_entrega_prevista 
    , fe.data_entrega_prometida 
    , fe.chv_data_entrega as data_entrega
    , b.erp_delivered_at as data_entrega_beagle
    from analytics_base.pnl_omni pos3
    left join dw_corporativo.ft_entrega fe on fe.pedido_beagle = pos3.id_pedido
    left join
    	(
        select 
        bso.number as pedido_beagle
        , bss.number as chv_entrega
        , bss.erp_delivered_at 
    	from nessie_cache.beagle_spree_orders bso
    	join nessie_cache.beagle_spree_shipments bss on bss.order_id = bso.id
        ) b on b.pedido_beagle = pos3.id_pedido and b.chv_entrega = fe.chv_entrega 
    where
    to_char(pos3.order_date, 'YYYY-MM-DD') >= '2020-01-01'
    union all
    select distinct
    df.nm_filial 
    , fe.pedido_beagle as pedido_beagle
    , fe.chv_data_emissao_nota_fiscal as data_pedido
    , fe.timestamp_aprovacao as data_pedido_aprovacao
    , fe.chv_entrega
    , fe.data_entrega_prevista 
    , fe.data_entrega_prometida 
    , fe.chv_data_entrega as data_entrega
    , b.erp_delivered_at as data_entrega_beagle
    from dw_corporativo.ft_entrega fe 
    left join dw_corporativo.dim_filial df on df.chv_filial = fe.chv_filial 
    left join
    	(
        select 
        bso.number as pedido_beagle
        , bss.number as chv_entrega
        , bss.erp_delivered_at 
    	from nessie_cache.beagle_spree_orders bso
    	join nessie_cache.beagle_spree_shipments bss on bss.order_id = bso.id
        ) b on b.pedido_beagle = fe.pedido_beagle and b.chv_entrega = fe.chv_entrega 
    inner join
    	(
    	select
        fpf.pedido_beagle as id_pedido
        from dw_corporativo.ft_pedido_faturado fpf
        left join dw_corporativo.dim_filial df on df.chv_filial = fpf.chv_filial
        where 
        df.nm_filial = 'CD Minihub São Paulo'
        and
        fpf.chv_data_emissao_nota_fiscal >= '2020-01-01'
        union
        select 
        pos3.id_pedido as id_pedido
        from analytics_base.pnl_omni pos3
        where
        to_char(pos3.order_date, 'YYYY-MM-DD') >= '2020-01-01'
        ) ppj on ppj.id_pedido = fe.pedido_beagle"


#sqlQuery_base_cotacoes_2020 <- 
#  "select
#    *
#    from dbt_anderson_mello.step04_base_cotacoes_2020"
#
#sqlQuery_base_cotacoes_2021_td <- 
#  "select
#    *
#    from dbt_anderson_mello.step04_base_cotacoes_2021-td"

    
    
### Abrir conexao / Radar queries / Fechar conexao

conn <- odbcDriverConnect(constring)

base_minihub_proprio <- sqlQuery(conn, sqlQuery_base_minihub_proprio)
base_minihub_parceiro <- sqlQuery(conn, sqlQuery_base_minihub_parceiro)
pedidos_brinde <- sqlQuery(conn, sqlQuery_pedidos_brinde)
pedidos_spot <- sqlQuery(conn, sqlQuery_base_spot)
prazo_entrega <- sqlQuery(conn, sqlQuery_base_prazo_entrega)
#base_cotacoes_carrinhos_2020 <- sqlQuery(conn, sqlQuery_base_cotacoes_2020)
#base_cotacoes_carrinhos_2021_td <- sqlQuery(conn, sqlQuery_base_cotacoes_2021_td)

close(conn)

### Exportação de bases

#base_cotacoes_carrinhos_2020 %>% write_rds(x = ., file = paste0('C:/Users/anderson.paiva/Documents/batchs_outputs_rds/base_cotacoes_carrinhos_2020','_',date(Sys.time()),'.rds'))
#base_cotacoes_carrinhos_2021_td %>% write_rds(x = ., file = paste0('C:/Users/anderson.paiva/Documents/batchs_outputs_rds/base_cotacoes_carrinhos_2021_td','_',date(Sys.time()),'.rds'))


### Carregar bases de excel

### Ajuste de bases

pedidos_brinde_v1 <- pedidos_brinde %>% 
  mutate(id_pedido = as.numeric(id_pedido),
         pedido_brinde = 'sim')

base_minihub_total <- base_minihub_proprio %>% 
  rbind(base_minihub_parceiro)

pedidos_com_split_v1 <- base_minihub_total %>% 
  mutate(full_petloveja = entrega_petloveja) %>% 
  select(id_pedido,
         full_petloveja) %>% 
  filter(full_petloveja == 'nao') %>% 
  distinct()

pedidos_com_split_v2 <- base_minihub_total %>% 
  left_join(pedidos_com_split_v1, by = "id_pedido") %>% 
  mutate(unidade_petloveja_split = unidade_petloveja)

pedidos_com_split_v3 <- pedidos_com_split_v2 %>% 
  select(id_pedido,
         unidade_petloveja_split,
         full_petloveja) %>% 
  filter(unidade_petloveja_split != 'CD Extrema',
         full_petloveja == 'nao') %>% 
  distinct()

base_minihub_total_v1 <- base_minihub_total %>%
  mutate(id_pedido = as.numeric(id_pedido)) %>% 
  left_join(pedidos_brinde_v1, by = "id_pedido") %>% 
  left_join(pedidos_com_split_v3, by = "id_pedido") %>% 
  mutate(unidade_petloveja_vf = ifelse(is.na(unidade_petloveja_split), unidade_petloveja, unidade_petloveja_split ))


base_minihub_total_v2 <- base_minihub_total_v1 %>%
  mutate(ano_mes_pedidos = paste0((substring(as.character(data_pedido),1,4)),
                                  (substring(as.character(data_pedido),6,7))),
         dia_pedidos = paste0((substring(as.character(data_pedido),1,4)),
                              (substring(as.character(data_pedido),6,7)),
                              (substring(as.character(data_pedido),9,10))),
         entrega_petloveja = ifelse(entrega_petloveja == 'sim', 1, 0),
         pedidos_brinde = ifelse(pedido_brinde == 'sim', 1, 0),
         pedidos_full_petloveja = ifelse((full_petloveja != 'nao'| is.na(full_petloveja)), 1, 0),
         ano_mes_td = ifelse(substring(as.character(data_pedido),9,10) < substring(as.character(today()),9,10), 'MTD', 'ND')) %>%
  select(unidade_petloveja_vf,
         ano_mes_pedidos,
         dia_pedidos,
         data_pedido,
         ano_mes_td,
         canal_venda,
         id_pedido,
         pedidos_brinde,
         pedidos_full_petloveja,
         entrega_petloveja,
         nm_municipio,
         chv_cliente,
         cliente_novo,
         receita_bruta_total,
         receita_bruta_frete)



### Analise

# Visao consolidada BH e Minu HUB SP

analise_minihub_total_dia <- base_minihub_total_v2 %>% 
  group_by(unidade_petloveja_vf, ano_mes_pedidos, dia_pedidos, ano_mes_td) %>%
  summarise(
    receita_total = sum(receita_bruta_total, na.rm = TRUE),
    receita_petloveja_full = sum(ifelse(pedidos_full_petloveja == 1, receita_bruta_total, 0), na.rm = TRUE),
    receita_petloveja_split = sum(ifelse((pedidos_full_petloveja == 0 & entrega_petloveja == 1), receita_bruta_total, 0), na.rm = TRUE),
    receita_cd_split = sum(ifelse((pedidos_full_petloveja == 0 & entrega_petloveja == 0), receita_bruta_total, 0), na.rm = TRUE),
    receita_frete_total = sum(receita_bruta_frete, na.rm = TRUE),
    receita_frete_petloveja_full = sum(ifelse(entrega_petloveja == 1, receita_bruta_frete, 0),na.rm = TRUE),
    receita_frete_petloveja_split = sum(ifelse(entrega_petloveja == 0, receita_bruta_frete, 0),na.rm = TRUE),
    receita_frete_cd_split = 0,
    qtd_pedidos_total = n_distinct(id_pedido),
    qtd_pedidos_petloveja_full = n_distinct(id_pedido),
    qtd_pedidos_petloveja_split = n_distinct(ifelse(entrega_petloveja == 0, id_pedido, 0))-1,
    qtd_pedidos_cd_split = 0,
    repres_pedidos_split = ifelse(qtd_pedidos_petloveja_split/qtd_pedidos_total == Inf, 0, qtd_pedidos_petloveja_split/qtd_pedidos_total),
    qtd_pedidos_brinde = n_distinct(ifelse(pedidos_brinde == 1,id_pedido,0)),
    tckt_medio_total = ifelse(receita_total/qtd_pedidos_total == Inf, 0, receita_total/qtd_pedidos_total),
    tckt_medio_petlove_split = ifelse(receita_petloveja_split/qtd_pedidos_petloveja_split == Inf, 0, receita_petloveja_split/qtd_pedidos_petloveja_split),
    tckt_medio_petloveja_full = ifelse(receita_petloveja_full/qtd_pedidos_petloveja_full == Inf, 0, receita_petloveja_full/qtd_pedidos_petloveja_full),
    tckt_medio_cd_split = ifelse(receita_cd_split/qtd_pedidos_cd_split == Inf, 0, receita_cd_split/qtd_pedidos_cd_split),
    clientes_totais = n_distinct(chv_cliente),
    clientes_novos = n_distinct(ifelse(cliente_novo == 1,id_pedido,0))) %>% 
  mutate(CONC1 = paste0(unidade_petloveja_vf, ano_mes_pedidos, dia_pedidos))

analise_pedidos_petolve_spot <- pedidos_spot %>% 
  mutate(unidade_petloveja_vf = as.character(unidade_petloveja),
         ano_mes_pedidos = as.character(ano_mes_pedidos),
         dia_pedidos = paste0((substring(as.character(dia_pedidos),1,4)),
                             (substring(as.character(dia_pedidos),6,7)),
                             (substring(as.character(dia_pedidos),9,10))),
         qtd_pedidos_spot = as.numeric(qtd_pedidos_spot),
         CONC1 = paste0(unidade_petloveja_vf, ano_mes_pedidos, dia_pedidos)) %>% 
  select(CONC1,
         qtd_pedidos_spot)

analise_minihub_total_dia_v2 <- analise_minihub_total_dia %>%
  left_join(analise_pedidos_petolve_spot, by = 'CONC1')

analise_minihub_total_dia_vf <- analise_minihub_total_dia_v2 %>% 
  select(unidade_petloveja_vf,
         ano_mes_pedidos,
         dia_pedidos,
         ano_mes_td,
         receita_total,
         receita_petloveja_full,
         receita_petloveja_split,
         receita_cd_split,
         receita_frete_total,
         receita_frete_petloveja_full,
         receita_frete_petloveja_split,
         receita_frete_cd_split,
         qtd_pedidos_total,
         qtd_pedidos_petloveja_full,
         qtd_pedidos_petloveja_split,
         qtd_pedidos_cd_split,
         repres_pedidos_split,
         qtd_pedidos_brinde,
         tckt_medio_total,
         tckt_medio_petloveja_full,
         tckt_medio_petlove_split,
         tckt_medio_cd_split,
         clientes_totais,
         clientes_novos,
         qtd_pedidos_spot)


# Analise de cotacoes com petloveja disponivel

#cotacoes_2101 <- read_rds('C:/Users/anderson.paiva/Documents/batchs_outputs_rds/base_cotacoes_carrinhos_2101_2021-08-31.rds')
#cotacoes_2102 <- read_rds('C:/Users/anderson.paiva/Documents/batchs_outputs_rds/base_cotacoes_carrinhos_2102_2021-08-31.rds')
#cotacoes_2103 <- read_rds('C:/Users/anderson.paiva/Documents/batchs_outputs_rds/base_cotacoes_carrinhos_2103_2021-08-31.rds')
#cotacoes_2104 <- read_rds('C:/Users/anderson.paiva/Documents/batchs_outputs_rds/base_cotacoes_carrinhos_2104_2021-08-31.rds')
#cotacoes_2105 <- read_rds('C:/Users/anderson.paiva/Documents/batchs_outputs_rds/base_cotacoes_carrinhos_2105_2021-08-31.rds')
#cotacoes_2106 <- read_rds('C:/Users/anderson.paiva/Documents/batchs_outputs_rds/base_cotacoes_carrinhos_2106_2021-08-31.rds')
#cotacoes_2107 <- read_rds('C:/Users/anderson.paiva/Documents/batchs_outputs_rds/base_cotacoes_carrinhos_2107_2021-08-31.rds')
#cotacoes_2108 <- read_rds('C:/Users/anderson.paiva/Documents/batchs_outputs_rds/base_cotacoes_carrinhos_2108_2021-09-01.rds')
#
#cotacoes_td <- 
#  bind_rows(cotacoes_2101,
#            cotacoes_2102,
#            cotacoes_2103,
#            cotacoes_2104,
#            cotacoes_2105,
#            cotacoes_2106,
#            cotacoes_2107,
#            cotacoes_2108)
#  
#cotacoes_td_v1 <- cotacoes_td %>% 
#  group_by(
#    created_at_month,
#    chosen_shipping_method,
#    canal_venda,
#    subcanal_venda,
#    elegivel_petloveja,
#    unidade_petloveja,
#    entrega0_method,
#    entrega0_0,
#    entrega0_1,
#    entrega1_method,
#    entrega1_0,
#    entrega1_1,
#    entrega2_method,
#    entrega2_0,
#    entrega2_1) %>%
#  summarise(
#    qtd_cotacoes = n_distinct(id_cotacao),
#    qtd_pedidos = n_distinct(id_pedido)) 


### Exportacao da base para excel

analise_minihub_total_dia_vf %>% writexl::write_xlsx(x = .,path = paste0('C:/Users/anderson.paiva/Documents/batchs_outputs/analise_minihub_total_dia','_', date(Sys.time()),'.xlsx'))
prazo_entrega %>% writexl::write_xlsx(x = .,path = paste0('C:/Users/anderson.paiva/Documents/batchs_outputs/base_prazo_entrega_petloveja','_', date(Sys.time()),'.xlsx'))
base_minihub_total_v2 %>% writexl::write_xlsx(x = .,path = paste0('C:/Users/anderson.paiva/Documents/batchs_outputs/base_minihub_total','_', date(Sys.time()),'.xlsx'))
#cotacoes_td_v1 %>% writexl::write_xlsx(x = .,path = paste0('C:/Users/anderson.paiva/Documents/batchs_outputs/analise_cotacoes','_', date(Sys.time()),'.xlsx'))

