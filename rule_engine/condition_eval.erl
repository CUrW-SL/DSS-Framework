%%% -------------------------------------------------------------------------------------------
%%% Author  : Hasitha.
%%% Description : Rule engine implementation.
%%% Created : November 13, 2015.
%%%--------------------------------------------------------------------------------------------
-module(condition_eval).

-define(LMS_SMS_POOL, pool_sms).
%%%---------------------------------------External api-----------------------------------------
-export([call_rule_engine/3, call_rule_engine/5, call_rule_engine/4,
    get_bill_value/3,
    get_redeem_points/2,
    get_discount_offers/2,
    get_exclusive_offers/2,
    get_rule_sql/1,
    get_secret_code/1,
    reward_point_enability/1, reward_point_enability/2,
    bonus_money_enability/1, bonus_money_enability/2,
    get_bucket_expiary_date/2,
    redeem_points/3,
    get_top_ten_partners/0,
    round/2,
    log_entry/4,
    get_condition_result/2,
    get_offer_schedular/2,
    get_notification_sql/1,
    reward_point_enability_batch/1, bonus_money_enability_batch/1]).
%%%--------------------------------------------------------------------------------------------

%%%---------------------------------------Include files----------------------------------------
-define(RULE_ENGINE_POOL, pool_rule_engine).
-define(CONNECTION_ARG, [{driver, oracle},
    {database, "(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = \"192.168.56.103\")(PORT = 1521)) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = XE)))"},
    {host, "192.168.56.103"},
    {user, "lms"},
    {password, "lms"},
    {port, 1521},
    {poolsize, 5},
    {default_pool, true},
    {table_info, true}]).

-define(RE_CATEGORY, 0).
-define(RE_POINTS, 1).
-define(RE_OFFERS, 2).
-define(RE_OPTIN, 3).
-define(RE_BILL, 4).
-define(GBL_OPTIN_RULEID, "optin_rule_id").
% -record(log_record,{function="no_function",inputs="no_input",output="no_output",result=0}).

-define(GBL_CONVERSION_RATE, "conv_rate").

-define(ERROR, {error, error}).
-define(EMPTY, {error, empty}).
-define(LOG_ERROR, {log, error}).
-define(LOG_SUCESS, {log, success}).
-define(LOG_EMPTY, {log, empty}).
-define(SQL_ERROR, "sql query result error").
-define(SQL_EMPTY, "sql query result empty").

-define(PARENT_SQL, "select cx_table.\"msisdn\" from \"Customer\" cx_table, ").
-define(PARENT_SQL1, "select cx_table.\"msisdn\" from \"Customer\" cx_table ").
-define(CON_SQL1, "(select \"msisdn\", ").
-define(CON_SQL2_1, "(\"value\") as ").
-define(CON_SQL2_2, " as ").
-define(CON_SQL3, " from ").
-define(CON_SQL4, " where ").
-define(CON_SQL5, " and ").
-define(CON_SQL7, " and \"parameter\"= ").
-define(CON_SQL6, " group by \"msisdn\") ").
-define(SQL_TABLE, "_table").

-define(EXE_FREQUENCY_DAILY, 2).
-define(EXE_FREQUENCY_WEEKLY, 3).
-define(EXE_FREQUENCY_MONTHLY, 4).
-define(EXE_FREQUENCY_QUATER, 5).
-define(EXE_FREQUENCY_HALF_YEARLY, 6).
-define(EXE_FREQUENCY_YEARLY, 7).

-define(INFO(X, Y), io:format("INFO{~p,~p, ~p,~p}: " ++ X ++ "~n", [date(), time(), ?MODULE, ?LINE] ++ Y)).
%%  -define(INFO(X, Y), ok).
-define(WARN(X, Y), io:format("WARN{~p,~p, ~p,~p}: " ++ X ++ "~n", [date(), time(), ?MODULE, ?LINE] ++ Y)).
-define(ERROR(X, Y), io:format("ERROR{~p,~p, ~p,~p}: " ++ X ++ "~n", [date(), time(), ?MODULE, ?LINE] ++ Y)).
-define(NONE(X, Y), ok).

%%%--------------------------------------------------------------------------------------------
call_rule_engine(?RE_CATEGORY, Msisdn, CategoryRuleId, CurrentCategoryId, Type) -> % 1-upgrade|0-downgrade
    get_category_change(CategoryRuleId, Msisdn, CurrentCategoryId, Type).

call_rule_engine(?RE_POINTS, MsisdnCatList, PointRuleId) ->
    get_bonus_batch(PointRuleId, MsisdnCatList);

call_rule_engine(?RE_OFFERS, Msisdn, OfferId) ->
    get_offer(OfferId, Msisdn);

call_rule_engine(?RE_OPTIN, Msisdn, RuleId) ->
    get_optin_customer(RuleId, Msisdn);

% call_rule_engine(?RE_INVENTORY, Msisdn,OfferId)->
%     get_inventory(OfferId,Msisdn);

call_rule_engine(_, _Msisdn, _Id) ->
    ?ERROR.

call_rule_engine(?RE_POINTS, Msisdn, PointRuleId, CatId) ->
    ?INFO("get_bonus|inputs:~p~n", [{Msisdn, PointRuleId}]),
    get_bonus(PointRuleId, Msisdn, CatId);
call_rule_engine(?RE_BILL, Msisdn, Points, OfferId) ->
    S = get_bill_value(OfferId, Msisdn, Points),
    ?INFO("~p", [{S}]),
    S.
%%%-------------------------------get exclusive offers functions--------------------------------
get_exclusive_offers(Msisdn, OfferId) ->
    get_offer(OfferId, Msisdn).

%%%----------------------------------get discount offers functions------------------------------
get_discount_offers(Msisdn, OfferId) ->
    get_offer(OfferId, Msisdn).

%%%----------------------------------get redeem functions---------------------------------------
get_redeem_points(OfferId, MSISDN) ->
    SqlQuery = lists:concat(["select \"points\",\"conversionOrder\" from \"Offer\" where \"Offer\".\"id\" = ", OfferId]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_redeem_points|execute_sql", lists:concat(["OfferId:", OfferId, "|MSISDN:", MSISDN]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "get_redeem_points|execute_sql", lists:concat(["OfferId:", OfferId, "|MSISDN:", MSISDN]), ?SQL_EMPTY),
            ?ERROR;
        {ok, [[Points, ConversionOrder] | _]} ->
            SqlQuery1 = lists:concat(["select \"id\" from \"OfferGroup\" where \"OfferGroup\".\"offerId\" = ", OfferId]),
            case execute_sql(SqlQuery1) of
                {error, Error} ->
                    log_entry(?LINE, "get_redeem_points|execute_sql", lists:concat(["OfferId:", OfferId, "|MSISDN:", MSISDN, "|Points:", Points, "|ConversionOrder:", ConversionOrder]), lists:concat([?SQL_ERROR, ":", Error])),
                    ?ERROR;
                {ok, []} ->
                    log_entry(?LINE, "get_redeem_points|execute_sql", lists:concat(["OfferId:", OfferId, "|MSISDN:", MSISDN, "|Points:", Points, "|ConversionOrder:", ConversionOrder]), ?SQL_EMPTY),
                    ?ERROR;
                {ok, [[OfferGroupId] | _]} ->
                    % ?INFO("OfferGroupId:~p~n",[OfferGroupId]),
                    case get_conversion_ratio(string:tokens(ConversionOrder, ","), MSISDN, OfferGroupId) of
                        {ok, empty} ->
                            log_entry(?LINE, "get_redeem_points|get_conversion_ratio", lists:concat(["OfferId:", OfferId, "|MSISDN:", MSISDN, "|OfferGroupId:", OfferGroupId, "|ConversionOrder:", ConversionOrder]), "ok,empty"),
                            ?ERROR;
                        {error, _Error} ->
                            log_entry(?LINE, "get_redeem_points|get_conversion_ratio", lists:concat(["OfferId:", OfferId, "|MSISDN:", MSISDN, "|OfferGroupId:", OfferGroupId, "|ConversionOrder:", ConversionOrder]), "error"),
                            ?ERROR;
                        ok ->
                            log_entry(?LINE, "get_redeem_points|get_conversion_ratio", lists:concat(["OfferId:", OfferId, "|MSISDN:", MSISDN, "|OfferGroupId:", OfferGroupId, "|ConversionOrder:", ConversionOrder]), "ok"),
                            ?ERROR;
                        Ratio ->
                            {OfferGroupId, Points, Points * Ratio}
                    end
            end

    end.

%%%----------------------------------get bill functions-----------------------------------------
get_bill_value(OfferId, MSISDN, Points) ->
    SqlQuery = lists:concat(["select \"id\" from \"OfferGroup\" where \"OfferGroup\".\"offerId\" = ", OfferId]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_bill_value|execute_sql", lists:concat(["OfferId:", OfferId, "|MSISDN:", MSISDN, "|Points:", Points]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "get_bill_value|execute_sql", lists:concat(["OfferId:", OfferId, "|MSISDN:", MSISDN, "|Points:", Points]), ?SQL_EMPTY),
            ?ERROR;
        {ok, [[OfferGroupId] | _]} ->
            ?INFO("OfferGroupId:~p~n", [OfferGroupId]),
            SqlQuery1 = lists:concat(["select \"conversionOrder\" from \"Offer\" where \"Offer\".\"id\" = ", OfferId]),
            case execute_sql(SqlQuery1) of
                {error, Error} ->
                    log_entry(?LINE, "get_bill_value|execute_sql", lists:concat(["OfferGroupId:", OfferGroupId]), lists:concat([?SQL_ERROR, ":", Error])),
                    ?ERROR;
                {ok, []} ->
                    log_entry(?LINE, "get_bill_value|execute_sql", lists:concat(["OfferGroupId:", OfferGroupId]), ?SQL_EMPTY),
                    ?ERROR;
                {ok, [[ConversionOrder] | _]} ->
                    case get_conversion_ratio(string:tokens(ConversionOrder, ","), MSISDN, OfferGroupId) of
                        {ok, empty} ->
                            log_entry(?LINE, "get_bill_value|get_conversion_ratio", lists:concat(["ConversionOrder:", ConversionOrder, "|MSISDN:", MSISDN, "|OfferGroupId:", OfferGroupId]), "ok,empty"),
                            ?ERROR;
                        {error, _Error} ->
                            log_entry(?LINE, "get_bill_value|get_conversion_ratio", lists:concat(["ConversionOrder:", ConversionOrder, "|MSISDN:", MSISDN, "|OfferGroupId:", OfferGroupId]), "error,Error"),
                            ?ERROR;
                        Ratio ->
                            {OfferGroupId, Points * Ratio, MSISDN} %%Ratio:(1point= ## BDT)
                    end
            end
    end.

get_conversion_ratio(ConversionOrderList, MSISDN, OfferGroupId) ->
    % ?INFO("ConversionOrderList:~p~n",[ConversionOrderList]),
    SqlQuery = lists:concat(["select \"categoryId\" from \"Customer\" where \"Customer\".\"msisdn\" = ", MSISDN]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_conversion_ratio|execute_sql", lists:concat(["ConversionOrderList:", ConversionOrderList, "|MSISDN:", MSISDN, "|OfferGroupId:", OfferGroupId]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "get_conversion_ratio|execute_sql", lists:concat(["ConversionOrderList:", ConversionOrderList, "|MSISDN:", MSISDN, "|OfferGroupId:", OfferGroupId]), ?SQL_EMPTY),
            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
            ?ERROR;
        {ok, [[CategoryId] | _]} ->
            % ?INFO("CategoryId:~p~n",[CategoryId]),
            get_ordered_ratio(ConversionOrderList, CategoryId, OfferGroupId)
    end.

get_ordered_ratio([Head | Tail], CategoryId, OfferGroupId) ->
    case Head of
        "3" ->
            ?INFO("ConversionOrderNum:3~n", []),
            case get_bcr_ratio(CategoryId) of
                {ok, empty} ->
                    get_ordered_ratio(Tail, CategoryId, OfferGroupId);
                {ok, undefined} ->
                    get_ordered_ratio(Tail, CategoryId, OfferGroupId);
                {error, _Error} ->
                    get_ordered_ratio(Tail, CategoryId, OfferGroupId);
                {ok, Ratio} ->
                    Ratio;
                _ ->
                    get_ordered_ratio(Tail, CategoryId, OfferGroupId)
            end;
        "2" ->
            ?INFO("ConversionOrderNum:2~n", []),
            case get_partner_ratio(OfferGroupId) of
                {ok, empty} ->
                    get_ordered_ratio(Tail, CategoryId, OfferGroupId);
                {ok, undefined} ->
                    get_ordered_ratio(Tail, CategoryId, OfferGroupId);
                {error, _Error} ->
                    get_ordered_ratio(Tail, CategoryId, OfferGroupId);
                {ok, Ratio} ->
                    Ratio;
                _ ->
                    get_default_ratio()
            end;
        "1" ->
            ?INFO("ConversionOrderNum:1~n", []),
            get_default_ratio();
        Else ->
            ?INFO("Unknown value:~p", [Else]),
            get_default_ratio()
    end;

get_ordered_ratio([], CategoryId, OfferGroupId) ->
    log_entry(?LINE, "get_ordered_ratio", lists:concat(["CategoryId:", CategoryId, "|OfferGroupId:", OfferGroupId]), "Conversion OrderList empty"),
    ?ERROR.

get_bcr_ratio(CategoryId) ->
    SqlQuery = lists:concat(["select \"pointsToBdt\" from \"PointConversion\" where SYSDATE>\"PointConversion\".\"startDateTime\"  and SYSDATE<\"PointConversion\".\"endDateTime\" and \"status\"=1 and \"PointConversion\".\"categoryId\" = ", CategoryId]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_bcr_ratio|execute_sql", lists:concat(["CategoryId:", CategoryId]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "get_bcr_ratio|execute_sql", lists:concat(["CategoryId:", CategoryId]), ?SQL_EMPTY),
            % ?INFO("Rule Engine error at line:~p~n",[?LINE]),
            ?ERROR;
        {ok, RatioList} ->
            % ?INFO("RatioList:~p~n",[RatioList]),
            % ?INFO("Ratio:~p~n",[lists:max(lists:merge(RatioList))]),
            {ok, lists:max(lists:merge(RatioList))}
    end.

get_partner_ratio(OffferGroupId) ->
    % ?INFO("Partner ratio.~n",[]),
    % SqlQuery=lists:concat(["select \"partnerId\" from \"OfferGroupPartnerBranch\" where \"OfferGroupPartnerBranch\".\"offerGroupId\" = ",OffferGroupId]),
    SqlQuery = lists:concat(["select \"partnerId\" from \"OfferGroupPartner\" where \"OfferGroupPartner\".\"offerGroupId\" = ", OffferGroupId]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_partner_ratio|execute_sql", lists:concat(["OffferGroupId:", OffferGroupId]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "get_partner_ratio|execute_sql", lists:concat(["OffferGroupId:", OffferGroupId]), ?SQL_EMPTY),
            ?ERROR;
        {ok, [[PartnerId] | _]} ->
            % ?INFO("PartnerId:~p~n",[PartnerId]),
            SqlQuery1 = lists:concat(["select \"conversionRate\" from \"Partner\" where \"Partner\".\"id\" = ", PartnerId]),
            case execute_sql(SqlQuery1) of
                {error, Error} ->
                    log_entry(?LINE, "get_partner_ratio|execute_sql", lists:concat(["PartnerId:", PartnerId]), lists:concat([?SQL_ERROR, ":", Error])),
                    ?ERROR;
                {ok, []} ->
                    log_entry(?LINE, "get_partner_ratio|execute_sql", lists:concat(["PartnerId:", PartnerId]), ?SQL_EMPTY),
                    ?ERROR;
                {ok, [[ConversionRate] | _]} ->
                    % ?INFO("ConversionRate:~p~n",[ConversionRate]),
                    {ok, ConversionRate}
            end
    end.

get_default_ratio() ->
    case lms_util:get_conf_value(?GBL_CONVERSION_RATE) of
        error ->
            ?ERROR;
        UrlStr ->
            lms_util:convert_to_float(UrlStr)
    end.

%%%--------------------------------get category functions--------------------------------------
get_category_change(RuleID, MSISDN, CurrentCategoryId, Type) ->
    ?INFO("Line:~p~n", [?LINE]),
    SqlQuery = lists:concat(["select case when exists (select 1 from \"CategorizationBlacklist\" where \"msisdn\"='", MSISDN, "')
                then 'Y' else 'N' end as rec_exist from dual"]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_category_change|execute_sql", lists:concat(["MSISDN:", MSISDN]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, [["Y"] | _]} ->
            ?INFO("BLack listed.~n", []),
            log_entry(?LINE, "get_category_change|execute_sql", lists:concat(["MSISDN:", MSISDN]), "msisdn black listed."),
            ?INFO("Line:~p~n", [?LINE]),
            ?ERROR;
        {ok, [["N"] | _]} ->
            ?INFO("Line:~p~n", [?LINE]),
            ?INFO("Not BLack listed.~n", []),
            get_category(RuleID, MSISDN, CurrentCategoryId, Type)
    end.

get_category(RuleID, MSISDN, CurrentCategoryId, Type) ->
    ?INFO("MSISDN:~p |RuleID:~p ~n", [MSISDN, RuleID]),
    SqlQuery = lists:concat(["select \"ruleId\",\"baseRuleId\",\"categoryId\" from \"CategoryRule\" where \"CategoryRule\".\"id\" = ", RuleID]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            ?INFO("line:~p~n", [?LINE]),
            log_entry(?LINE, "get_category|execute_sql", lists:concat(["MSISDN:", MSISDN, "|RuleID:", RuleID]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "get_category|execute_sql", lists:concat(["MSISDN:", MSISDN, "|RuleID:", RuleID]), ?SQL_EMPTY),
            ?INFO("line:~p~n", [?LINE]),
            ?ERROR;
        % {ok,Result} ->
        %     ?INFO("Result:~p~n",[Result])
        {ok, [[CategoryRuleId, undefined, NewCategoryId] | _]} ->
            case get_condition_result(CategoryRuleId, MSISDN) of
                {ok, true} ->
                    ?INFO("Line:~p~n", [?LINE]),
                    category_change(CurrentCategoryId, NewCategoryId, Type);
                _ ->
                    ?INFO("line:~p~n", [?LINE]),
                    log_entry(?LINE, "get_category|get_condition_result", lists:concat(["MSISDN:", MSISDN, "|CategoryRuleId:", CategoryRuleId]), "CategoryRule mismatch"),
                    ?ERROR
            end;
        {ok, [[CategoryRuleId, BaseRuleId, NewCategoryId] | _]} ->
            ?INFO("Line:~p~n", [?LINE]),
            case get_condition_result(BaseRuleId, MSISDN) of
                {ok, true} ->
                    ?INFO("Line:~p~n", [?LINE]),
                    case get_condition_result(CategoryRuleId, MSISDN) of
                        {ok, true} ->
                            ?INFO("Line:~p~n", [?LINE]),
                            category_change(CurrentCategoryId, NewCategoryId, Type);
                        {ok, false} ->
                            ?INFO("line:~p~n", [?LINE]),
                            log_entry(?LINE, "get_category|get_condition_result", lists:concat(["MSISDN:", MSISDN, "|CategoryRuleId:", CategoryRuleId]), ",Category Rule mismatch"),
                            ?ERROR;
                        _ ->
                            ?INFO("line:~p~n", [?LINE]),
                            log_entry(?LINE, "get_category|get_condition_result", lists:concat(["MSISDN:", MSISDN, "|CategoryRuleId:", CategoryRuleId]), "Category Rule mismatch|other error"),
                            ?ERROR
                    end;
                {ok, false} ->
                    log_entry(?LINE, "get_category|get_condition_result", lists:concat(["MSISDN:", MSISDN, "|BaseRuleId:", BaseRuleId]), "baseRule mismatch"),
                    ?INFO("line:~p~n", [?LINE]),
                    ?ERROR;
                _ ->
                    log_entry(?LINE, "get_category|get_condition_result", lists:concat(["MSISDN:", MSISDN, "|BaseRuleId:", BaseRuleId]), "baseRule mismatch|other error"),
                    ?INFO("line:~p~n", [?LINE]),
                    ?ERROR
            end
    end.

category_change(CurrentCategoryId, NewCategoryId, Type) ->
    SqlQuery = lists:concat(["select \"new_priority\",\"current_priority\" from (select \"priority\" as \"new_priority\" from \"Category\" where \"id\"=", NewCategoryId, "),(select \"priority\" as \"current_priority\" from \"Category\" where \"id\"=", CurrentCategoryId, ")"]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "category_change|execute_sql", lists:concat(["CurrentCategoryId:", CurrentCategoryId, "|NewCategoryId:", NewCategoryId, "|Type:", Type]), lists:concat([?SQL_ERROR, ":", Error])),
            ?INFO("line:~p~n", [?LINE]),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "category_change|execute_sql", lists:concat(["CurrentCategoryId:", CurrentCategoryId, "|NewCategoryId:", NewCategoryId, "|Type:", Type]), ?SQL_EMPTY),
            ?INFO("line:~p~n", [?LINE]),
            ?ERROR;
        {ok, [[undefined, _] | _]} ->
            log_entry(?LINE, "category_change|execute_sql", lists:concat(["CurrentCategoryId:", CurrentCategoryId, "|NewCategoryId:", NewCategoryId, "|Type:", Type]), "new_priority undefined"),
            ?INFO("line:~p~n", [?LINE]),
            ?ERROR;
        {ok, [[_, undefined] | _]} ->
            log_entry(?LINE, "category_change|execute_sql", lists:concat(["CurrentCategoryId:", CurrentCategoryId, "|NewCategoryId:", NewCategoryId, "|Type:", Type]), "current_priority undefined"),
            ?INFO("line:~p~n", [?LINE]),
            ?ERROR;
        {ok, [[New_priority, Current_priority] | _]} ->
            ?INFO("line:~p~n", [?LINE]),
            ?INFO("priority levels:~p~n", [{New_priority, Current_priority}]),
            case Type of
                0 ->
                    if
                        (New_priority =< Current_priority) -> %upgrade/same category
                            ?INFO("line:~p~n", [?LINE]),
                            {ok, true, false};
                        (New_priority > Current_priority) -> %downgrade
                            ?INFO("line:~p~n", [?LINE]),
                            {ok, true, true}
                    end;
                1 ->
                    if
                        (New_priority < Current_priority) -> %upgrade
                            ?INFO("line:~p~n", [?LINE]),
                            {ok, true, true};
                        (New_priority >= Current_priority) -> %downgrade/same category
                            ?INFO("line:~p~n", [?LINE]),
                            {ok, true, false}
                    end;
                _ ->
                    log_entry(?LINE, "category_change|execute_sql", lists:concat(["CurrentCategoryId:", CurrentCategoryId, "|NewCategoryId:", NewCategoryId, "|Type:", Type]), "type mismatch"),
                    ?ERROR
            end
    end.

%%%--------------------------------get notification sql functions------------------------------
get_notification_sql(Id) ->
    ?INFO("get_notification_sql|inputs|Id :~p~n", [Id]),
    SqlQuery = lists:concat(["select \"baseType\",\"baseValue\" from \"Notification\" where \"id\" = ", Id]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_notification_sql|execute_sql", lists:concat(["Id:", Id]), lists:concat([?SQL_ERROR, ":", Error])),
            ?INFO("ERROR :~p~n", [Error]),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "get_notification_sql|execute_sql", lists:concat(["Id:", Id]), ?SQL_EMPTY),
            ?INFO("SQL_EMPTY:~p~n", [?SQL_EMPTY]),
            ?ERROR;
        {ok, [[BaseType, BaseValue] | _]} ->
            ?INFO("{BaseType,BaseValue}:~p~n", [{BaseType, BaseValue}]),
            case BaseType of
                0 ->
                    ?INFO("BaseType:~p~n", [BaseType]),
                    {ok, "select \"msisdn\" from \"Customer\""};
                1 ->
                    ?INFO("BaseType:~p~n", [BaseType]),
                    case get_category_sql(string:tokens(BaseValue, ","), " where ", true) of
                        {empty, empty} ->
                            log_entry(?LINE, "get_notification_sql|get_category_sql", lists:concat(["BaseValue:", BaseValue]), "no category ids found"),
                            ?ERROR;
                        {ok, OrPart} ->
                            ?INFO("OrPart:~p~n", [OrPart]),
                            {ok, lists:concat(["select \"msisdn\" from \"Customer\" ", OrPart])}
                    end;
                2 ->
                    ?INFO("BaseType:~p~n", [BaseType]),
                    {ok, lists:concat(["select \"msisdn\" from \"NotificationNumbers\" where \"notificationId\" = \"", Id])};
                3 ->
                    ?INFO("BaseType:~p~n", [BaseType]),
                    get_rule_sql(BaseValue)
            end
    end.

get_category_sql([CategoryId | Rest], OrPart, First) ->
    case First of
        true ->
            NewOrPart = lists:concat([OrPart, " \"categoryId\" = '", CategoryId, "'"]),
            get_category_sql(Rest, NewOrPart, false);
        _ ->
            NewOrPart = lists:concat([OrPart, " or \"categoryId\" = '", CategoryId, "'"]),
            get_category_sql(Rest, NewOrPart, false)
    end;

get_category_sql([], OrPart, First) ->
    case First of
        true ->
            ?INFO("OrPart:~p~n", [OrPart]),
            {empty, empty};
        false ->
            ?INFO("OrPart:~p~n", [OrPart]),
            {ok, OrPart}
    end.

%%%--------------------------------get offer schedular functions-------------------------------
get_offer_schedular(OfferID, MSISDN) ->
    SqlQuery = lists:concat(["select \"id\",\"groupId\",\"offerValue\" from \"OfferGroup\" where \"offerId\" = ", OfferID, " and \"offerValueType\" = 3 and \"groupType\"= 1"]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_offer_schedular|execute_sql", lists:concat(["OfferID:", OfferID, "|MSISDN:", MSISDN]), lists:concat([?SQL_ERROR, ":", Error])),
            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
            ?ERROR;
        {ok, []} ->
            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
            log_entry(?LINE, "get_offer_schedular|execute_sql", lists:concat(["OfferID:", OfferID, "|MSISDN:", MSISDN]), ?SQL_EMPTY),
            ?ERROR;
        {ok, [[OfferGroupId, GroupId, OfferValue] | _]} ->
            ?INFO("{OfferGroupId,GroupId,OfferValue}:~p~n", [{OfferGroupId, GroupId, OfferValue}]),
            case get_condition_result(GroupId, MSISDN) of
                {ok, true} ->
                    case OfferValue of
                        undefined ->
                            {ok, true};
                        _ ->
                            ?INFO("OfferValue:~p~n", [OfferValue]),
                            SqlQuery1 = lists:concat(["select case when exists (select 1 from \"OfferAllowedNumbers\" where \"offerGroupId\"=", OfferGroupId, " and \"msisdn\"='", MSISDN, "') then 'true' else 'false'  end as \"exists\" from dual"]),
                            case execute_sql(SqlQuery1) of
                                {error, Error} ->
                                    log_entry(?LINE, "get_offer_schedular|execute_sql", lists:concat(["OfferGroupId:", OfferGroupId]), lists:concat([?SQL_ERROR, ":", Error])),
                                    ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                                    ?ERROR;
                                {ok, []} ->
                                    ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                                    log_entry(?LINE, "get_offer_schedular|execute_sql", lists:concat(["OfferGroupId:", OfferGroupId]), ?SQL_EMPTY),
                                    ?ERROR;
                                {ok, [[Exists] | _]} ->
                                    ?INFO("Exists:~p~n", [Exists]),
                                    case Exists of
                                        "true" ->
                                            {ok, exist};
                                        "false" ->
                                            SqlQuery2 = lists:concat(["select count(\"msisdn\") as \"count\" from \"OfferAllowedNumbers\" where \"offerGroupId\" = ", OfferGroupId]),
                                            case execute_sql(SqlQuery2) of
                                                {error, Error} ->
                                                    log_entry(?LINE, "get_offer_schedular|execute_sql", lists:concat(["OfferGroupId:", OfferGroupId]), lists:concat([?SQL_ERROR, ":", Error])),
                                                    ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                                                    ?ERROR;
                                                {ok, []} ->
                                                    ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                                                    log_entry(?LINE, "get_offer_schedular|execute_sql", lists:concat(["OfferGroupId:", OfferGroupId]), ?SQL_EMPTY),
                                                    ?ERROR;
                                                {ok, [[Count] | _]} ->
                                                    ?INFO("Count:~p~n", [Count]),
                                                    if
                                                        Count < OfferValue ->
                                                            ?INFO("{Count,OfferValue}:~p~n", [{Count, OfferValue}]),
                                                            {ok, true};
                                                        true ->
                                                            ?INFO("{Count,OfferValue}:~p~n", [{Count, OfferValue}]),
                                                            ?INFO("Limit exeeded.~p~n", []),
                                                            {ok, exeed}
                                                    end
                                            end
                                    end
                            end
                    end;
                _ ->
                    ?INFO("line:~p~n", [?LINE]),
                    log_entry(?LINE, "get_offer_schedular|get_condition_result", lists:concat(["GroupId:", GroupId, "|MSISDN:", MSISDN]), "condition_mismatch")
            end
    end.

%%%--------------------------------get offer related functions---------------------------------
get_offer(OfferID, MSISDN) ->
    SqlQuery = lists:concat(["select \"id\",\"groupType\",\"groupId\",\"offerValueType\" from \"OfferGroup\" where \"OfferGroup\".\"offerId\" = ", OfferID]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_offer|execute_sql", lists:concat(["OfferID:", OfferID, "|MSISDN:", MSISDN]), lists:concat([?SQL_ERROR, ":", Error])),
            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
            ?ERROR;
        {ok, []} ->
            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
            log_entry(?LINE, "get_offer|execute_sql", lists:concat(["OfferID:", OfferID, "|MSISDN:", MSISDN]), ?SQL_EMPTY),
            ?ERROR;
        {ok, List} ->
            ?INFO("line:~p~n", [?LINE]),
            evaluate_offer(List, {}, MSISDN, OfferID)
    end.

evaluate_offer([[Id, GroupType, GroupId, OfferValueType] | Rest], _Return, MSISDN, OfferID) ->
    case GroupType of
        0 ->
            ?INFO("category ", []),
            SqlQuery1 = lists:concat(["select \"categoryId\" from \"Customer\" where \"Customer\".\"msisdn\" = '", MSISDN, "'"]),
            case execute_sql(SqlQuery1) of
                {error, Error} ->
                    ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                    log_entry(?LINE, "get_offer|execute_sql", lists:concat(["Id:", Id, "|GroupType:", GroupType, "|GroupId:", GroupId]), lists:concat([?SQL_ERROR, ":", Error])),
                    evaluate_offer(Rest, ?ERROR, MSISDN, OfferID);
                {ok, []} ->
                    ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                    log_entry(?LINE, "get_offer|execute_sql", lists:concat(["Id:", Id, "|GroupType:", GroupType, "|GroupId:", GroupId]), ?SQL_EMPTY),
                    evaluate_offer(Rest, ?ERROR, MSISDN, OfferID);
                {ok, [[CategoryId] | _]} ->
                    case lists:member(integer_to_list(CategoryId), string:tokens(GroupId, ",")) of
                        true ->
                            ?INFO("true.~n", []),
                            {ok, Id};
                        _ ->
                            ?INFO("false.~n", []),
                            evaluate_offer(Rest, {ok, false}, MSISDN, OfferID)
                    end
            end;
        1 ->
            ?INFO("rule~n", []),
            case get_condition_result(GroupId, MSISDN) of
                {ok, true} ->
                    ?INFO("line:~p~n", [?LINE]),
                    SqlQuery3 = lists:concat(["select \"offerType\" from \"Offer\" where \"Offer\".\"id\" = '", OfferID, "'"]),
                    case execute_sql(SqlQuery3) of
                        {error, Error} ->
                            log_entry(?LINE, "get_offer|execute_sql", lists:concat(["GroupType:", GroupType, "|GroupId:", GroupId]), lists:concat([?SQL_ERROR, ":", Error])),
                            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                            evaluate_offer(Rest, ?ERROR, MSISDN, OfferID);
                        {ok, []} ->
                            log_entry(?LINE, "get_offer|execute_sql", lists:concat(["GroupType:", GroupType, "|GroupId:", GroupId]), ?SQL_EMPTY),
                            evaluate_offer(Rest, ?ERROR, MSISDN, OfferID);
                        {ok, [[OfferType] | _]} ->
                            ?INFO("OfferType:~p~n", [OfferType]),
                            case OfferType of
                                2 ->
                                    case OfferValueType of
                                        3 ->
                                            ?INFO("OfferValueType:~p~n", [OfferValueType]),
                                            SqlQuery4 = lists:concat(["select * from \"OfferAllowedNumbers\" where \"msisdn\" = '", MSISDN, "'", " and offerGroupId='", Id, "'"]),
                                            case execute_sql(SqlQuery4) of
                                                {error, Error} ->
                                                    log_entry(?LINE, "get_offer|execute_sql", lists:concat(["MSISDN:", MSISDN, "|Id:", Id]), lists:concat([?SQL_ERROR, ":", Error])),
                                                    evaluate_offer(Rest, ?ERROR, MSISDN, OfferID);
                                                {ok, []} ->
                                                    log_entry(?LINE, "get_offer|execute_sql", lists:concat(["MSISDN:", MSISDN, "|Id:", Id]), ?SQL_EMPTY),
                                                    evaluate_offer(Rest, ?ERROR, MSISDN, OfferID);
                                                {ok, List} ->
                                                    ?INFO("List:~p~n", [List]),
                                                    {ok, Id}
                                            end;
                                        _ ->
                                            {ok, Id}
                                    end;
                                _ ->
                                    {ok, Id}
                            end
                    end;
                _ ->
                    ?INFO("line:~p~n", [?LINE]),
                    log_entry(?LINE, "get_offer|get_condition_result", lists:concat(["GroupId:", GroupId, "|MSISDN:", MSISDN]), "condition_mismatch"),
                    evaluate_offer(Rest, {ok, false}, MSISDN, OfferID)
            end;
        2 ->
            ?INFO("table~n", []),
            SqlQuery2 = lists:concat(["select * from \"OfferAllowedNumbers\",(select \"id\" from \"OfferGroup\" where \"offerId\"= ", OfferID, ") \"offerGroupIds\" where \"msisdn\" = ", MSISDN, " and \"offerGroupId\"=\"offerGroupIds\".\"id\""]),
            ?INFO("SqlQuery2: ~p~n", [SqlQuery2]),
            case execute_sql(SqlQuery2) of
                {error, Error} ->
                    log_entry(?LINE, "get_offer|execute_sql", lists:concat(["Id:", Id, "|GroupType:", GroupType, "|GroupId:", GroupId]), lists:concat([?SQL_ERROR, ":", Error])),
                    ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                    evaluate_offer(Rest, ?ERROR, MSISDN, OfferID);
                {ok, []} ->
                    log_entry(?LINE, "get_offer|execute_sql", lists:concat(["Id:", Id, "|GroupType:", GroupType, "|GroupId:", GroupId]), ?SQL_EMPTY),
                    evaluate_offer(Rest, {ok, false}, MSISDN, OfferID);
                {ok, [List]} ->
                    ?INFO("List:~p~n", [List]),
                    {ok, Id}
            end;
        _ ->
            ?INFO("line:~p~n", [?LINE]),
            evaluate_offer(Rest, ?ERROR, MSISDN, OfferID)
    end;

evaluate_offer([], NewReturn, _, _) ->
    ?INFO("line:~p~n", [?LINE]),
    NewReturn.

%%%--------------------------------get bonus points/money related functions---------------------------------
get_bonus(RuleID, MSISDN, CatId) ->
    ?INFO("Line number:~p~n", [?LINE]),
    ?INFO("Line number:~p|inputs{RuleID,MSISDN}:~p~n", [?LINE, {RuleID, MSISDN}]),
    SqlQuery = lists:concat(["select \"categoryId\",\"ruleType\",\"pointType\",\"baseRuleId\",\"behaviourParam\",\"behaviourType\",\"behaviourValue\" from \"PointRule\" where \"id\" = ", RuleID, " and \"status\" = 1"]),
    ?INFO("SqlQuery:~p~n", [SqlQuery]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_bonus|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), lists:concat([?SQL_ERROR, ":", Error])),
            ?INFO("line:~p~n", [?LINE]),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "get_bonus|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), ?SQL_EMPTY),
            ?INFO("line:~p~n", [?LINE]),
            ?ERROR;
        {ok, [[CategoryId, RuleType, PointType, BaseRuleId, BehaviourParam, BehaviourType, BehaviourValue] | _]} ->
            ?INFO("Line number:~p~n", [?LINE]),
            case RuleType of
                0 -> % global
                    ?INFO("Line number:~p~n", [?LINE]),
                    case CatId of
                        CategoryId ->
                            ?INFO("Line number:~p~n", [?LINE]),
                            case [BehaviourParam, BehaviourType, BehaviourValue] of
                                [undefined, _, _] ->
                                    log_entry(?LINE, "get_bonus|execute_sql", lists:concat(["BehaviourParam:", BehaviourParam, "|BehaviourType:", BehaviourType, "|BehaviourValue:", BehaviourValue]), "BehaviourParam undefined");
                                [_, undefined, _] ->
                                    log_entry(?LINE, "get_bonus|execute_sql", lists:concat(["BehaviourParam:", BehaviourParam, "|BehaviourType:", BehaviourType, "|BehaviourValue:", BehaviourValue]), "BehaviourType undefined");
                                [_, _, undefined] ->
                                    log_entry(?LINE, "get_bonus|execute_sql", lists:concat(["BehaviourParam:", BehaviourParam, "|BehaviourType:", BehaviourType, "|BehaviourValue:", BehaviourValue]), "BehaviourValue undefined");
                                [BehaviourParam, BehaviourType, BehaviourValue] ->
                                    ?INFO("Behaviour set:~p~n", [{BehaviourParam, BehaviourType, BehaviourValue}]),
                                    ?INFO("Line number:~p~n", [?LINE]),
                                    SqlQuery2 = lists:concat(["select \"value\" from \"CustomerDailyDetails\" where \"msisdn\"='", MSISDN, "' and \"parameter\"='", BehaviourParam, "' and \"actDate\"= trunc(sysdate-1)"]),
                                    ?INFO("Line number:~p~n", [?LINE]),
                                    ?INFO("SqlQuery2:~p~n", [SqlQuery2]),
                                    case execute_sql(SqlQuery2) of
                                        {error, Error} ->
                                            log_entry(?LINE, "get_bonus|execute_sql", lists:concat(["BehaviourParam:", BehaviourParam, "|BehaviourType:", BehaviourType, "|BehaviourValue:", BehaviourValue, "|Msisdn:", MSISDN]), lists:concat([?SQL_ERROR, ":", Error])),
                                            ?INFO("Line number:~p~n", [?LINE]),
                                            ?ERROR;
                                        {ok, []} ->
                                            log_entry(?LINE, "get_bonus|execute_sql", lists:concat(["BehaviourParam:", BehaviourParam, "|BehaviourType:", BehaviourType, "|BehaviourValue:", BehaviourValue, "|Msisdn:", MSISDN]), ?SQL_EMPTY),
                                            ?INFO("Line number:~p~n", [?LINE]),
                                            ?ERROR;
                                        {ok, [[ParamValue] | _]} ->
                                            ?INFO("Line number:~p~n", [?LINE]),
                                            ?INFO("BehaviourType:~p~n", [BehaviourType]),
                                            ?INFO("ParamValue:~p~n", [ParamValue]),
                                            case BehaviourType of
                                                0 -> % fixed.
                                                    ?INFO("Line number:~p~n", [?LINE]),
                                                    case PointType of
                                                        0 -> % point
                                                            ?INFO("Line number:~p~n", [?LINE]),
                                                            {ok, round(BehaviourValue)};
                                                        1 -> % bonus money
                                                            ?INFO("Line number:~p~n", [?LINE]),
                                                            {ok, BehaviourValue}
                                                    end;
                                                1 -> % percentage.
                                                    ?INFO("Line number:~p~n", [?LINE]),
                                                    case PointType of
                                                        0 -> % point
                                                            ?INFO("Line number:~p~n", [?LINE]),
                                                            if
                                                                is_integer(ParamValue) ->
                                                                    ?INFO("Line number:~p~n", [?LINE]),
                                                                    {ok, round(BehaviourValue * (ParamValue / 100))};
                                                                is_float(ParamValue) ->
                                                                    ?INFO("Line number:~p~n", [?LINE]),
                                                                    {ok, round(BehaviourValue * (ParamValue / 100))};
                                                                is_list(ParamValue) ->
                                                                    ?INFO("Line number:~p~n", [?LINE]),
                                                                    case catch list_to_integer(ParamValue) of
                                                                        Val when is_integer(Val) ->
                                                                            ?INFO("Line number:~p~n", [?LINE]),
                                                                            ?INFO("Val:~p~n", [Val]),
                                                                            {ok, round(BehaviourValue * (list_to_integer(ParamValue) / 100))};
                                                                        _ ->
                                                                            ?INFO("Line number:~p~n", [?LINE]),
                                                                            {ok, round(BehaviourValue * (list_to_float(ParamValue) / 100))}
                                                                    end
                                                            end;
                                                        1 -> % bonus money
                                                            ?INFO("Line number:~p~n", [?LINE]),
                                                            if
                                                                is_integer(ParamValue) ->
                                                                    ?INFO("Line number:~p~n", [?LINE]),
                                                                    {ok, BehaviourValue * (ParamValue / 100)};
                                                                is_float(ParamValue) ->
                                                                    ?INFO("Line number:~p~n", [?LINE]),
                                                                    {ok, BehaviourValue * (ParamValue / 100)};
                                                                is_list(ParamValue) ->
                                                                    ?INFO("Line number:~p~n", [?LINE]),
                                                                    case catch list_to_integer(ParamValue) of
                                                                        Val when is_integer(Val) ->
                                                                            ?INFO("Line number:~p~n", [?LINE]),
                                                                            ?INFO("Val:~p~n", [Val]),
                                                                            {ok, BehaviourValue * (list_to_integer(ParamValue) / 100)};
                                                                        _ ->
                                                                            ?INFO("Line number:~p~n", [?LINE]),
                                                                            {ok, BehaviourValue * (list_to_float(ParamValue) / 100)}
                                                                    end
                                                            end
                                                    end
                                            end
                                    end
                            end;
                        _ ->
                            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                            log_entry(?LINE, "get_bonus|execute_sql", lists:concat(["CategoryId:", CategoryId, "|RuleType:", RuleType, "|PointType:", PointType, "|BaseRuleId:", BaseRuleId, "|BehaviourParam:", BehaviourParam, "|BehaviourType:", BehaviourType, "|BehaviourValue:", BehaviourValue]), "categoryId mismatched"),
                            ?ERROR
                    end;
                1 -> % bussiness
                    ?INFO("Line number:~p~n", [?LINE]),
                    case [CategoryId, BaseRuleId] of
                        [CategoryId, undefined] ->
                            ?INFO("Line number:~p~n", [?LINE]),
                            case CatId of
                                CategoryId ->
                                    case get_point_rule_action(RuleID, MSISDN) of
                                        {ok, Value} ->
                                            case PointType of
                                                0 -> % point
                                                    ?INFO("Line number:~p~n", [?LINE]),
                                                    {ok, round(Value)};
                                                1 -> % bonus money
                                                    ?INFO("Line number:~p~n", [?LINE]),
                                                    {ok, Value}
                                            end;
                                        _ ->
                                            log_entry(?LINE, "get_bonus|get_point_rule_action", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "point error"),
                                            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                                            ?ERROR
                                    end;
                                _ ->
                                    log_entry(?LINE, "get_bonus|get_point_rule_action", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "CategoryId mismatched"),
                                    ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                                    ?ERROR
                            end;
                        [undefined, BaseRuleId] ->
                            ?INFO("Line number:~p~n", [?LINE]),
                            case get_condition_result(BaseRuleId, MSISDN) of
                                {ok, true} ->
                                    ?INFO("Line number:~p~n", [?LINE]),
                                    case get_point_rule_action(RuleID, MSISDN) of
                                        {ok, Value} ->
                                            ?INFO("Line number:~p~n", [?LINE]),
                                            ?INFO("Value:~p~n", [Value]),
                                            case PointType of
                                                0 -> % point
                                                    ?INFO("Line number:~p~n", [?LINE]),
                                                    {ok, round(Value)};
                                                1 -> % bonus money
                                                    ?INFO("Line number:~p~n", [?LINE]),
                                                    {ok, Value}
                                            end;
                                        _ ->
                                            log_entry(?LINE, "get_bonus|get_condition_result|get_point_rule_action", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "point error"),
                                            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                                            ?ERROR
                                    end;
                                {ok, false} ->
                                    log_entry(?LINE, "get_bonus|get_condition_result|get_point_rule_action", lists:concat(["BaseRuleId:", BaseRuleId, "|MSISDN:", MSISDN]), "baseRule condition_mismatch"),
                                    ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                                    ?ERROR;
                                Error ->
                                    ?INFO("Error~p~n", [Error]),
                                    log_entry(?LINE, "get_bonus|get_condition_result|get_point_rule_action", lists:concat(["BaseRuleId:", BaseRuleId, "|MSISDN:", MSISDN]), "other error"),
                                    ?ERROR
                            end;
                        [CategoryId, BaseRuleId] ->
                            ?INFO("Line number:~p~n", [?LINE]),
                            log_entry(?LINE, "get_bonus|get_condition_result|get_point_rule_action", lists:concat(["CategoryId:", CategoryId, "|BaseRuleId:", BaseRuleId]), "Both categoryId and baseRuleId can't exists same time."),
                            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                            ?ERROR
                    end
            end
    end.

get_bonus_batch(RuleID, MsisdnCatList) ->
    %% 	MSISDN = batch,
    ?INFO("Line number:~p~n", [?LINE]),
    ?INFO("Line number:~p|inputs{RuleID,MSISDN}:~p~n", [?LINE, {RuleID, batch}]),
    SqlQuery = lists:concat(["select \"categoryId\",\"ruleType\",\"pointType\",\"baseRuleId\",\"behaviourParam\",\"behaviourType\",\"behaviourValue\" from \"PointRule\" where \"id\" = ", RuleID, " and \"status\" = 1"]),
    ?INFO("SqlQuery:~p~n", [SqlQuery]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_bonus|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", batch]), lists:concat([?SQL_ERROR, ":", Error])),
            ?INFO("line:~p~n", [?LINE]),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "get_bonus|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", batch]), ?SQL_EMPTY),
            ?INFO("line:~p~n", [?LINE]),
            ?ERROR;
        {ok, [[CategoryId, RuleType, PointType, BaseRuleId, BehaviourParam, BehaviourType, BehaviourValue] | _]} ->
            ?INFO("Line number:~p~n", [?LINE]),
            case RuleType of
                0 -> % global
                    ?INFO("Line number:~p~n", [?LINE]),
                    {CatList, NotCatList} = category_msisdn_list(MsisdnCatList, CategoryId, [], []),
                    if
                        CatList /= [] ->
                            MStr = string:join(CatList, ","),
                            ?INFO("Line number:~p~n", [?LINE]),
                            case [BehaviourParam, BehaviourType, BehaviourValue] of
                                [undefined, _, _] ->
                                    log_entry(?LINE, "get_bonus|execute_sql", lists:concat(["BehaviourParam:", BehaviourParam, "|BehaviourType:", BehaviourType, "|BehaviourValue:", BehaviourValue]), "BehaviourParam undefined");
                                [_, undefined, _] ->
                                    log_entry(?LINE, "get_bonus|execute_sql", lists:concat(["BehaviourParam:", BehaviourParam, "|BehaviourType:", BehaviourType, "|BehaviourValue:", BehaviourValue]), "BehaviourType undefined");
                                [_, _, undefined] ->
                                    log_entry(?LINE, "get_bonus|execute_sql", lists:concat(["BehaviourParam:", BehaviourParam, "|BehaviourType:", BehaviourType, "|BehaviourValue:", BehaviourValue]), "BehaviourValue undefined");
                                [BehaviourParam, BehaviourType, BehaviourValue] ->
                                    ?INFO("Behaviour set:~p~n", [{BehaviourParam, BehaviourType, BehaviourValue}]),
                                    ?INFO("Line number:~p~n", [?LINE]),
                                    SqlQuery2 = lists:concat(["select \"msisdn\", \"value\" from \"CustomerDailyDetails\" where \"msisdn\" in (", MStr, ") and \"parameter\"='", BehaviourParam, "' and \"actDate\"= trunc(sysdate-1)"]),
                                    ?INFO("Line number:~p~n", [?LINE]),
                                    ?INFO("SqlQuery2:~p~n", [SqlQuery2]),
                                    case execute_sql(SqlQuery2) of
                                        {error, Error} ->
                                            log_entry(?LINE, "get_bonus|execute_sql", lists:concat(["BehaviourParam:", BehaviourParam, "|BehaviourType:", BehaviourType, "|BehaviourValue:", BehaviourValue, "|Msisdn:", batch]), lists:concat([?SQL_ERROR, ":", Error])),
                                            ?INFO("Line number:~p~n", [?LINE]),
                                            {[], CatList ++ NotCatList};
                                        {ok, []} ->
                                            log_entry(?LINE, "get_bonus|execute_sql", lists:concat(["BehaviourParam:", BehaviourParam, "|BehaviourType:", BehaviourType, "|BehaviourValue:", BehaviourValue, "|Msisdn:", batch]), ?SQL_EMPTY),
                                            ?INFO("Line number:~p~n", [?LINE]),
                                            {[], CatList ++ NotCatList};
                                        {ok, ValList} ->
                                            ?INFO("Line number:~p~n", [?LINE]),
                                            ?INFO("BehaviourType:~p~n", [BehaviourType]),
                                            ?INFO("ParamValue:~p~n", [ValList]),
                                            FunV = fun([Ms, ParamValue], SucList) ->
                                                MSISDN = lists:concat([Ms]),
                                                case BehaviourType of
                                                    0 -> % fixed.
                                                        ?INFO("Line number:~p~n", [?LINE]),
                                                        case PointType of
                                                            0 -> % point
                                                                ?INFO("Line number:~p~n", [?LINE]),
                                                                SucList ++ [{MSISDN, round(BehaviourValue)}];
                                                            1 -> % bonus money
                                                                ?INFO("Line number:~p~n", [?LINE]),
                                                                SucList ++ [{MSISDN, BehaviourValue}]
                                                        end;
                                                    1 -> % percentage.
                                                        ?INFO("Line number:~p~n", [?LINE]),
                                                        case PointType of
                                                            0 -> % point
                                                                ?INFO("Line number:~p~n", [?LINE]),
                                                                if
                                                                    is_integer(ParamValue) ->
                                                                        ?INFO("Line number:~p~n", [?LINE]),
                                                                        SucList ++ [{MSISDN, round(BehaviourValue * (ParamValue / 100))}];
                                                                    is_float(ParamValue) ->
                                                                        ?INFO("Line number:~p~n", [?LINE]),
                                                                        SucList ++ [{MSISDN, round(BehaviourValue * (ParamValue / 100))}];
                                                                    is_list(ParamValue) ->
                                                                        ?INFO("Line number:~p~n", [?LINE]),
                                                                        case catch list_to_integer(ParamValue) of
                                                                            Val when is_integer(Val) ->
                                                                                ?INFO("Line number:~p~n", [?LINE]),
                                                                                ?INFO("Val:~p~n", [Val]),
                                                                                SucList ++ [{MSISDN, round(BehaviourValue * (list_to_integer(ParamValue) / 100))}];
                                                                            _ ->
                                                                                ?INFO("Line number:~p~n", [?LINE]),
                                                                                SucList ++ [{MSISDN, round(BehaviourValue * (list_to_float(ParamValue) / 100))}]
                                                                        end
                                                                end;
                                                            1 -> % bonus money
                                                                ?INFO("Line number:~p~n", [?LINE]),
                                                                if
                                                                    is_integer(ParamValue) ->
                                                                        ?INFO("Line number:~p~n", [?LINE]),
                                                                        SucList ++ [{MSISDN, BehaviourValue * (ParamValue / 100)}];
                                                                    is_float(ParamValue) ->
                                                                        ?INFO("Line number:~p~n", [?LINE]),
                                                                        SucList ++ [{MSISDN, BehaviourValue * (ParamValue / 100)}];
                                                                    is_list(ParamValue) ->
                                                                        ?INFO("Line number:~p~n", [?LINE]),
                                                                        case catch list_to_integer(ParamValue) of
                                                                            Val when is_integer(Val) ->
                                                                                ?INFO("Line number:~p~n", [?LINE]),
                                                                                ?INFO("Val:~p~n", [Val]),
                                                                                SucList ++ [{MSISDN, BehaviourValue * (list_to_integer(ParamValue) / 100)}];
                                                                            _ ->
                                                                                ?INFO("Line number:~p~n", [?LINE]),
                                                                                SucList ++ [{MSISDN, BehaviourValue * (list_to_float(ParamValue) / 100)}]
                                                                        end
                                                                end
                                                        end
                                                end
                                                   end,
                                            SucMsList = lists:foldl(FunV, [], ValList),
                                            {SMList, _} = lists:unzip(SucMsList),
                                            NotMList = lists:subtract(CatList, SMList),
                                            {SucMsList, NotMList ++ NotCatList}
                                    end
                            end;
                        true ->
                            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                            log_entry(?LINE, "get_bonus|execute_sql", lists:concat(["CategoryId:", CategoryId, "|RuleType:", RuleType, "|PointType:", PointType, "|BaseRuleId:", BaseRuleId, "|BehaviourParam:", BehaviourParam, "|BehaviourType:", BehaviourType, "|BehaviourValue:", BehaviourValue]), "categoryId mismatched"),
                            {[], NotCatList}
                    end;
                1 -> % bussiness
                    ?INFO("Line number:~p~n", [?LINE]),
                    case [CategoryId, BaseRuleId] of
                        [CategoryId, undefined] ->
                            ?INFO("Line number:~p~n", [{?LINE, CategoryId}]),
                            {CatList, CatPointList, NotCatList} = category_msisdn_point_list(MsisdnCatList, CategoryId, [], [], []),
                            if
                                CatList /= [] ->
                                    MStr = string:join(CatList, ","),
                                    MsPointList = get_point_rule_action_batch(RuleID, CatList, CatPointList, MStr),
                                    FunPT = fun({Ms, Pt}) ->
                                        case PointType of
                                            0 -> % point
                                                ?INFO("Line number:~p~n", [?LINE]),
                                                {Ms, round(Pt)};
                                            1 -> % bonus money
                                                ?INFO("Line number:~p~n", [?LINE]),
                                                {Ms, Pt}
                                        end
                                            end,
                                    NewMsPointList = lists:map(FunPT, MsPointList),
                                    {NewMsPointList, NotCatList};
                                true ->
                                    log_entry(?LINE, "get_bonus|get_point_rule_action", lists:concat(["RuleID:", RuleID, "|MSISDN:", batch]), "CategoryId mismatched"),
                                    ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                                    {[], NotCatList}
                            end;
                        [undefined, BaseRuleId] ->
                            ?INFO("Line number:~p~n", [?LINE]),
                            FunLM = fun({MSISDN, _Cat, _}, {PtList, NotList}) ->
                                case get_condition_result(BaseRuleId, batch) of
                                    {ok, true} ->
                                        ?INFO("Line number:~p~n", [?LINE]),
                                        case get_point_rule_action(RuleID, MSISDN) of
                                            {ok, Value} ->
                                                ?INFO("Line number:~p~n", [?LINE]),
                                                ?INFO("Value:~p~n", [Value]),
                                                case PointType of
                                                    0 -> % point
                                                        ?INFO("Line number:~p~n", [?LINE]),
                                                        {PtList ++ [{MSISDN, round(Value)}], NotList};
                                                    1 -> % bonus money
                                                        ?INFO("Line number:~p~n", [?LINE]),
                                                        {PtList ++ [{MSISDN, Value}], NotList}
                                                end;
                                            _ ->
                                                log_entry(?LINE, "get_bonus|get_condition_result|get_point_rule_action", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "point error"),
                                                ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                                                PtList ++ [MSISDN]
                                        end;
                                    {ok, false} ->
                                        log_entry(?LINE, "get_bonus|get_condition_result|get_point_rule_action", lists:concat(["BaseRuleId:", BaseRuleId, "|MSISDN:", MSISDN]), "baseRule condition_mismatch"),
                                        ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                                        PtList ++ [MSISDN];
                                    Error ->
                                        ?INFO("Error~p~n", [Error]),
                                        log_entry(?LINE, "get_bonus|get_condition_result|get_point_rule_action", lists:concat(["BaseRuleId:", BaseRuleId, "|MSISDN:", MSISDN]), "other error"),
                                        PtList ++ [MSISDN]
                                end
                                    end,
                            lists:foldl(FunLM, {[], []}, MsisdnCatList);
                        [CategoryId, BaseRuleId] ->
                            ?INFO("Line number:~p~n", [?LINE]),
                            log_entry(?LINE, "get_bonus|get_condition_result|get_point_rule_action", lists:concat(["CategoryId:", CategoryId, "|BaseRuleId:", BaseRuleId]), "Both categoryId and baseRuleId can't exists same time."),
                            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                            {[], MsisdnCatList}
                    end
            end
    end.

category_msisdn_list([], _Cat, CatList, UnList) ->
    {CatList, UnList};
category_msisdn_list([{M, Cat, _} | R], Cat, CatList, UnList) ->
    category_msisdn_list(R, Cat, CatList ++ [M], UnList);
category_msisdn_list([{M, _, _} | R], Cat, CatList, UnList) ->
    category_msisdn_list(R, Cat, CatList, UnList ++ [M]).

category_msisdn_point_list([], _Cat, CatList, CatPointList, UnList) ->
    {CatList, CatPointList, UnList};
category_msisdn_point_list([{M, Cat, _} | R], Cat, CatList, CatPointList, UnList) ->
    category_msisdn_point_list(R, Cat, CatList ++ [M], CatPointList ++ [{M, 0}], UnList);
category_msisdn_point_list([{M, _, _} | R], Cat, CatList, CatPointList, UnList) ->
    category_msisdn_point_list(R, Cat, CatList, CatPointList, UnList ++ [M]).

get_point_rule_action(RuleID, MSISDN) ->
    ?INFO("Line number:~p~n", [?LINE]),
    SqlQuery = lists:concat(["select \"id\",\"calculationType\",\"pointParamId\",\"pointParameter\" from \"PointRuleAction\" where \"PointRuleAction\".\"pointRuleId\" = ", RuleID]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            ?INFO("Line number:~p~n", [?LINE]),
            log_entry(?LINE, "get_point_rule_action|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            ?INFO("Line number:~p~n", [?LINE]),
            log_entry(?LINE, "get_point_rule_action|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), ?SQL_EMPTY),
            ?ERROR;
        {ok, ActionList} ->
            ?INFO("Line number:~p~n", [?LINE]),
            ?INFO("ActionList:~p~n", [ActionList]),
            case get_points(MSISDN, ActionList, 0) of
                {ok, Value} ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    {ok, Value};
                _ ->
                    ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                    log_entry(?LINE, "get_point_rule_action|get_points", lists:concat(["MSISDN:", MSISDN, "|ActionList:", ActionList]), "other error"),
                    ?ERROR
            end
    end.
get_point_rule_action_batch(RuleID, MsisdnList, MsPointList, MsStr) ->
    ?INFO("Line number:~p~n", [?LINE]),
    SqlQuery = lists:concat(["select \"id\",\"calculationType\",\"pointParamId\",\"pointParameter\" from \"PointRuleAction\" where \"PointRuleAction\".\"pointRuleId\" = ", RuleID]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            ?INFO("Line number:~p~n", [?LINE]),
            log_entry(?LINE, "get_point_rule_action|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", batch]), lists:concat([?SQL_ERROR, ":", Error])),
            MsPointList;
        {ok, []} ->
            ?INFO("Line number:~p~n", [?LINE]),
            log_entry(?LINE, "get_point_rule_action|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", batch]), ?SQL_EMPTY),
            MsPointList;
        {ok, ActionList} ->
            ?INFO("Line number:~p~n", [?LINE]),
            ?INFO("ActionList:~p~n", [ActionList]),
            get_points_batch(MsisdnList, MsPointList, MsStr, ActionList)
    end.

get_points(MSISDN, [Head | Rest], Total) ->
    case Head of
        % [PointRuleActionId,4,undefined,_ ] ->
        %     case calculate_points(PointRuleActionId,4,false,MSISDN) of
        %         {error,error}->
        %             insert_log_record(#log_record{function=lists:concat(["get_points|calculate_points|Line:",?LINE]),inputs=lists:concat(["PointRuleActionId:",PointRuleActionId,"|MSISDN:",MSISDN,"|ParamValue:false|CalculationType:",4]),output="error",result=0}),
        %             NewTotal=Total;
        %         {ok,Value}->
        %             NewTotal=Total+Value
        %     end;
        [PointRuleActionId, 4, ParamId, _] ->
            ?INFO("Line number:~p~n", [?LINE]),
            % SqlQuery=lists:concat(["select \"paramName\" from \"RuleVariableSet\" where \"id\" = '",ParamId,"' and \"valueType\"='DATE'"]),
            SqlQuery = lists:concat(["select \"paramName\" from \"RuleVariableSet\" where \"id\" = '", ParamId, "'"]),
            ?INFO("SqlQuery:~p~n", [SqlQuery]),
            case execute_sql(SqlQuery) of
                {error, Error} ->
                    log_entry(?LINE, "get_points", lists:concat(["PointRuleActionId:", PointRuleActionId, "|CalculationType:", 4, "|ParamId:", ParamId]), lists:concat([?SQL_ERROR, ":", Error])),
                    ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                    NewTotal = Total;
                {ok, []} ->
                    log_entry(?LINE, "get_points", lists:concat(["PointRuleActionId:", PointRuleActionId, "|CalculationType:", 4, "|ParamId:", ParamId]), ?SQL_EMPTY),
                    ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                    NewTotal = Total;
                {ok, [[ParamName] | _]} ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    ?INFO("ParamName:~p~n", [ParamName]),
                    SqlQuery1 = lists:concat(["select \"", ParamName, "\" from \"Customer\" where \"msisdn\"='", MSISDN, "'"]),
                    ?INFO("SqlQuery1:~p~n", [SqlQuery1]),
                    case execute_sql(SqlQuery1) of
                        {error, Error} ->
                            log_entry(?LINE, "get_points|execute_sql", lists:concat(["ParamName:", ParamName, "|MSISDN:", MSISDN]), lists:concat([?SQL_ERROR, ":", Error])),
                            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                            NewTotal = Total;
                        {ok, []} ->
                            log_entry(?LINE, "get_points|execute_sql", lists:concat(["ParamName:", ParamName, "|MSISDN:", MSISDN]), ?SQL_EMPTY),
                            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                            NewTotal = Total;
                        {ok, [[{datetime, {{_, Month, Day}, _}}] | _]} ->
                            ?INFO("Line number:~p~n", [?LINE]),
                            case date() of
                                {_, Month, Day} ->
                                    ?INFO("Line number:~p~n", [?LINE]),
                                    SqlQuery2 = lists:concat(["select \"amount\" from \"PointRuleConfig\" where \"pointRuleActionId\"=", PointRuleActionId]),
                                    ?INFO("SqlQuery2:~p~n", [SqlQuery2]),
                                    case execute_sql(SqlQuery2) of
                                        {error, Error} ->
                                            log_entry(?LINE, "get_points|execute_sql", lists:concat(["PointRuleActionId:", PointRuleActionId]), lists:concat([?SQL_ERROR, ":", Error])),
                                            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                                            NewTotal = Total;
                                        {ok, []} ->
                                            log_entry(?LINE, "get_points|execute_sql", lists:concat(["PointRuleActionId:", PointRuleActionId]), ?SQL_EMPTY),
                                            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                                            NewTotal = Total;
                                        {ok, [[Value] | _]} ->
                                            ?INFO("Line number:~p~n", [?LINE]),
                                            ?INFO("Value:~p~n", [Value]),
                                            NewTotal = Total + Value
                                    end;
                                _ ->
                                    ?INFO("Line number:~p~n", [?LINE]),
                                    NewTotal = Total
                            end;
                        _ ->
                            ?INFO("Line number:~p~n", [?LINE]),
                            log_entry(?LINE, "get_points|execute_sql", lists:concat(["ParamName:", ParamName, "|MSISDN:", MSISDN]), "specialday_mismatch."),
                            NewTotal = Total
                    end
            end;
        [PointRuleActionId, CalculationType, ParamId, _] ->
            ?INFO("Line number:~p~n", [?LINE]),
            case calculate_param_value(ParamId, MSISDN) of
                {error, error} ->
                    log_entry(?LINE, "get_points|calculate_param_value", lists:concat(["ParamId:", ParamId, "|MSISDN:", MSISDN]), "error"),
                    ?INFO("Line number:~p~n", [?LINE]),
                    NewTotal = Total;
                {ok, [{_, undefined}]} ->
                    log_entry(?LINE, "get_points|calculate_param_value", lists:concat(["ParamId:", ParamId, "|MSISDN:", MSISDN]), "ParamValue undefined"),
                    ?INFO("Line number:~p~n", [?LINE]),
                    NewTotal = Total;
                {ok, [{_, ParamValue}]} ->
                    case calculate_points(PointRuleActionId, CalculationType, ParamValue, MSISDN) of
                        {error, error} ->
                            log_entry(?LINE, "get_points|calculate_param_value|calculate_points", lists:concat(["PointRuleActionId:", PointRuleActionId, "|MSISDN:", MSISDN, "|ParamValue:", ParamValue, "|CalculationType:", CalculationType]), "error"),
                            ?INFO("Line number:~p~n", [?LINE]),
                            NewTotal = Total;
                        {ok, Value} ->
                            ?INFO("NewTotal:~p~n", [(Total + Value)]),
                            ?INFO("Line number:~p~n", [?LINE]),
                            NewTotal = Total + Value
                    end;
                _ ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    NewTotal = Total
            end;
        _ ->
            ?INFO("Line number:~p~n", [?LINE]),
            NewTotal = Total
    end,
    ?INFO("Line number:~p~n", [?LINE]),
    get_points(MSISDN, Rest, NewTotal);

get_points(MSISDN, [], Total) ->
    ?INFO("Line number:~p~n", [?LINE]),
    if
        Total > 0 ->
            ?INFO("Line number:~p~n", [?LINE]),
            {ok, Total};
        Total == 0 ->
            ?INFO("Line number:~p~n", [?LINE]),
            {ok, Total};
        true ->
            ?INFO("Line number:~p~n", [?LINE]),
            log_entry(?LINE, "get_points", lists:concat(["Total:", Total, "|MSISDN:", MSISDN]), "Total can't be negative"),
            ?ERROR
    end.

%%% ----------------- GET POINTS Batch ---------------------
get_points_batch(MsisdnList, MsPointList, MsStr, [Head | Rest]) ->
%% 	MSISDN = batch,
    case Head of
        [PointRuleActionId, 4, ParamId, _] ->
            ?INFO("Line number:~p~n", [?LINE]),
            % SqlQuery=lists:concat(["select \"paramName\" from \"RuleVariableSet\" where \"id\" = '",ParamId,"' and \"valueType\"='DATE'"]),
            SqlQuery = lists:concat(["select \"paramName\" from \"RuleVariableSet\" where \"id\" = '", ParamId, "'"]),
            ?INFO("SqlQuery:~p~n", [SqlQuery]),
            case execute_sql(SqlQuery) of
                {error, Error} ->
                    log_entry(?LINE, "get_points", lists:concat(["PointRuleActionId:", PointRuleActionId, "|CalculationType:", 4, "|ParamId:", ParamId]), lists:concat([?SQL_ERROR, ":", Error])),
                    ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                    NewMsPointList = MsPointList;
                {ok, []} ->
                    log_entry(?LINE, "get_points", lists:concat(["PointRuleActionId:", PointRuleActionId, "|CalculationType:", 4, "|ParamId:", ParamId]), ?SQL_EMPTY),
                    ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                    NewMsPointList = MsPointList;
                {ok, [[ParamName] | _]} ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    ?INFO("ParamName:~p~n", [ParamName]),
                    SqlQuery1 = lists:concat(["select \"msisdn\" from \"Customer\" where \"msisdn\" in (", MsStr, ") and to_char( sysdate, 'mm.dd' ) = to_char( \"", ParamName, "\", 'mm.dd' )"]),
                    ?INFO("SqlQuery1:~p~n", [SqlQuery1]),
                    case execute_sql(SqlQuery1) of
                        {error, Error} ->
                            log_entry(?LINE, "get_points|execute_sql", lists:concat(["ParamName:", ParamName, "|MSISDN:", batch]), lists:concat([?SQL_ERROR, ":", Error])),
                            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                            NewMsPointList = MsPointList;
                        {ok, []} ->
                            log_entry(?LINE, "get_points|execute_sql", lists:concat(["ParamName:", ParamName, "|MSISDN:", batch]), ?SQL_EMPTY),
                            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                            NewMsPointList = MsPointList;
                        %%                         {ok,[[{datetime,{{_ ,Month,Day},_ }}]|_ ]} ->
                        {ok, MsParaList} ->
                            ?INFO("Line number:~p~n", [?LINE]),
                            SqlQuery2 = lists:concat(["select \"amount\" from \"PointRuleConfig\" where \"pointRuleActionId\"=", PointRuleActionId]),
                            ?INFO("SqlQuery2:~p~n", [SqlQuery2]),
                            case execute_sql(SqlQuery2) of
                                {error, Error} ->
                                    log_entry(?LINE, "get_points|execute_sql", lists:concat(["PointRuleActionId:", PointRuleActionId]), lists:concat([?SQL_ERROR, ":", Error])),
                                    ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                                    NewMsPointList = MsPointList;
                                {ok, []} ->
                                    log_entry(?LINE, "get_points|execute_sql", lists:concat(["PointRuleActionId:", PointRuleActionId]), ?SQL_EMPTY),
                                    ?INFO("Rule Engine error at line:~p~n", [?LINE]),
                                    NewMsPointList = MsPointList;
                                {ok, [[Value] | _]} ->
                                    ?INFO("Line number:~p~n", [?LINE]),
                                    ?INFO("Value:~p~n", [Value]),
                                    FunCP = fun([Ms], CurMsPointList) ->
                                        case lists:keysearch(Ms, 1, CurMsPointList) of
                                            {value, {Ms, Cpoints}} ->
                                                lists:keyreplace(Ms, 1, CurMsPointList, {Ms, Cpoints + Value});
                                            false ->
                                                CurMsPointList
                                        end
                                            end,
                                    NewMsPointList = lists:foldl(FunCP, MsPointList, MsParaList)
                            end;
                        _ ->
                            ?INFO("Line number:~p~n", [?LINE]),
                            log_entry(?LINE, "get_points|execute_sql", lists:concat(["ParamName:", ParamName, "|MSISDN:", batch]), "specialday_mismatch."),
                            NewMsPointList = MsPointList
                    end
            end;
        [PointRuleActionId, CalculationType, ParamId, _] ->
            ?INFO("Line number:~p~n", [?LINE]),
            FunBO = fun({MSISDN, CPoint}, CurMsPointList) ->
                case calculate_param_value(ParamId, MSISDN) of
                    {error, error} ->
                        log_entry(?LINE, "get_points|calculate_param_value", lists:concat(["ParamId:", ParamId, "|MSISDN:", MSISDN]), "error"),
                        ?INFO("Line number:~p~n", [?LINE]),
                        CurMsPointList ++ [{MSISDN, CPoint}];
                    {ok, [{_, undefined}]} ->
                        log_entry(?LINE, "get_points|calculate_param_value", lists:concat(["ParamId:", ParamId, "|MSISDN:", MSISDN]), "ParamValue undefined"),
                        ?INFO("Line number:~p~n", [?LINE]),
                        CurMsPointList ++ [{MSISDN, CPoint}];
                    {ok, [{_, ParamValue}]} ->
                        case calculate_points(PointRuleActionId, CalculationType, ParamValue, MSISDN) of
                            {error, error} ->
                                log_entry(?LINE, "get_points|calculate_param_value|calculate_points", lists:concat(["PointRuleActionId:", PointRuleActionId, "|MSISDN:", MSISDN, "|ParamValue:", ParamValue, "|CalculationType:", CalculationType]), "error"),
                                ?INFO("Line number:~p~n", [?LINE]),
                                CurMsPointList ++ [{MSISDN, CPoint}];
                            {ok, Value} ->
                                ?INFO("NewTotal:~p~n", [(CPoint + Value)]),
                                ?INFO("Line number:~p~n", [?LINE]),
                                CurMsPointList ++ [{MSISDN, CPoint + Value}]
                        end;
                    _ ->
                        ?INFO("Line number:~p~n", [?LINE]),
                        CurMsPointList
                end
                    end,
            NewMsPointList = lists:foldl(FunBO, [], MsPointList);
        _ ->
            ?INFO("Line number:~p~n", [?LINE]),
            NewMsPointList = MsPointList
    end,
    ?INFO("Line number:~p~n", [?LINE]),
    get_points_batch(MsisdnList, NewMsPointList, MsStr, Rest);

get_points_batch(_MsisdnList, MsPointList, _MsStr, []) ->
    MsPointList.
%%% ----------------- GET POINTS Batch ---------------------

calculate_param_value(ParamId, MSISDN) ->
    ?INFO("INPUTS|ParamId:~p|MSISDN:~p~n", [ParamId, MSISDN]),
    SqlQuery = lists:concat(["select * from \"RuleVariableSet\" where \"RuleVariableSet\".\"id\" = ", ParamId]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "calculate_param_value|execute_sql", lists:concat(["ParamId:", ParamId, "|MSISDN:", MSISDN]), lists:concat([?SQL_ERROR, ":", Error])),
            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "calculate_param_value|execute_sql", lists:concat(["ParamId:", ParamId, "|MSISDN:", MSISDN]), ?SQL_EMPTY),
            ?INFO("Rule Engine error at line:~p~n", [?LINE]),
            ?ERROR;
        {ok, List} ->
            ?INFO("List:~p~n", [List]),
            case generate_query_set(List, MSISDN, []) of
                {ok, ResultMap} ->
                    ?INFO("ResultMap:~p~n", [ResultMap]),
                    case generate_sql_query(ResultMap, [], []) of
                        {ok, {SqlSet, ParamsSet}} ->
                            CompleteQuery = lists:concat(["select ", ParamsSet, " from ", SqlSet]),
                            ?INFO("CompleteQuery:~p~n", [CompleteQuery]),
                            case run_sql_query(CompleteQuery) of
                                {ok, [ResultList]} ->
                                    ?INFO("ResultList:~p~n", [ResultList]),
                                    case generate_result_set(ResultList, string:tokens(ParamsSet, ","), []) of
                                        {ok, ResultsMap} ->
                                            ?INFO("ResultMap:~p~n", [ResultsMap]),
                                            {ok, ResultsMap};
                                        Error ->
                                            ?INFO("Error:~p~n", [Error]),
                                            log_entry(?LINE, "calculate_param_value|generate_result_set", lists:concat(["ResultList:", ResultList, "|ParamsSet:", ParamsSet]), "error"),
                                            ?ERROR
                                    end;
                                _ ->
                                    log_entry(?LINE, "calculate_param_value|run_sql_query", "CompleteQuery", "error"),
                                    ?ERROR
                            end;
                        _ ->
                            log_entry(?LINE, "calculate_param_value|generate_sql_query", "ResultMap", "error"),
                            ?ERROR
                    end;
                _ ->
                    log_entry(?LINE, "calculate_param_value|generate_query_set", "List", "error"),
                    ?ERROR
            end
    end.

% calculate_points(PointRuleActionId,4,_,MSISDN)->
%     SqlQuery=lists:concat(["select \"ruleId\",\"amount\",\"amountType\" from \"PointRuleConfig\" where \"PointRuleConfig\".\"pointRuleActionId\" = ",PointRuleActionId]),
%     case execute_sql(SqlQuery) of
%         {error,Error} ->
%             log_entry(?LINE,"calculate_points|execute_sql",lists:concat(["PointRuleActionId:",PointRuleActionId,"|MSISDN:",MSISDN,"|CalculationType:4"]),lists:concat([?SQL_ERROR,":",Error])),
%             ?ERROR;
%         {ok,[]} ->
%             log_entry(?LINE,"calculate_points|execute_sql",lists:concat(["PointRuleActionId:",PointRuleActionId,"|MSISDN:",MSISDN,"|CalculationType:4"]),?SQL_EMPTY),
%             ?ERROR;
%         {ok,List} ->
%             ?INFO("List:~p~n",[List]),
%             calculate_incremental_points(List,MSISDN,0)
%     end;

calculate_points(PointRuleActionId, CalculationType, ParamValue, _MSISDN) ->
    ?INFO("PointRuleActionId:~p|CalculationType:~p|ParamValue:~p~n",
        [PointRuleActionId, CalculationType, ParamValue]),
    SqlQuery = lists:concat(["select \"ruleId\",\"amount\",\"amountType\" from \"PointRuleConfig\" where \"PointRuleConfig\".\"pointRuleActionId\" = ", PointRuleActionId]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "calculate_points|execute_sql", lists:concat(["PointRuleActionId:", PointRuleActionId, "|ParamValue:", ParamValue, "|CalculationType:", CalculationType]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "calculate_points|execute_sql", lists:concat(["PointRuleActionId:", PointRuleActionId, "|ParamValue:", ParamValue, "|CalculationType:", CalculationType]), ?SQL_EMPTY),
            ?ERROR;
        {ok, List} ->
            case CalculationType of
                1 ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    slab_points(List, ParamValue, 0);  %% calculate using a ratio.
                2 ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    graduate_points(List, ParamValue, 0); %% calculate using a percentage.
                3 ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    value_points(List, ParamValue, 0); %% direct value/ratio/percentage
                % 4 ->
                %     specialday_points(List,ParamValue,0);
                _ ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    log_entry(?LINE, "calculate_points", lists:concat(["PointRuleActionId:", PointRuleActionId, "|ParamValue:", ParamValue, "|CalculationType:", CalculationType]), "CalculationType mismatched"),
                    ?ERROR
            end
    end.

calculate_incremental_points([[RuleID, Amount, AmountType] | Rest], MSISDN, Total) ->
    % ?INFO("RuleID:~p|Amount:~p|AmountType:~p~n",[RuleID,Amount,AmountType]),
    % ?INFO("Case condition:~p~n",[get_increment_result(RuleID,MSISDN)]),
    case get_increment_result(RuleID, MSISDN) of
        {ok, true, [{_, Value1}, {_, Value2}]} ->
            Increment = Value2 - Value1,
            % ?INFO("Increment:~p~n",[Increment]),
            % ?INFO("Total:~p~n",[Total]),
            case AmountType of
                0 ->
                    NewTotal = Total + Amount;
                1 ->
                    NewTotal = Total + (Increment * (Amount / 100));
                2 ->
                    NewTotal = Total + (Increment / Amount)
            end;
        {ok, false, _} ->
            log_entry(?LINE, "calculate_incremental_points|get_increment_result", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "not incremental"),
            NewTotal = -100000000;
        _ ->
            log_entry(?LINE, "calculate_incremental_points|get_increment_result", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "error"),
            NewTotal = -100000000
    end,
    calculate_incremental_points(Rest, MSISDN, NewTotal);

calculate_incremental_points([], MSISDN, NewTotal) ->
    ?INFO("NewTotal:~p~n", [NewTotal]),
    if
        (NewTotal < 0) ->
            log_entry(?LINE, "calculate_incremental_points", lists:concat(["NewTotal:", NewTotal, "|MSISDN:", MSISDN]), "total cannot be negative"),
            ?ERROR;
        true ->
            {ok, NewTotal}
    end.

get_increment_result(RuleID, MSISDN) ->
    ?INFO("RuleID:~p|MSISDN:~p~n", [RuleID, MSISDN]),
    SqlQuery = lists:concat(["select \"finalSet\" from \"Rule\"  where \"Rule\".\"id\" = ", RuleID]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_increment_result|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "get_increment_result|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), ?SQL_EMPTY),
            ?ERROR;
        {ok, [[Condition] | _]} ->
            ?INFO("Condition:~p~n", [Condition]),
            case generate_query(RuleID, MSISDN) of
                {ok, Map} ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    case get_condition_rule(Condition, Map) of
                        {ok, NewCondition} ->
                            ?INFO("Line number:~p~n", [?LINE]),
                            case erl_scan:string(lists:concat(["Result=", NewCondition, "."])) of
                                {ok, ErlTokens, _} ->
                                    ?INFO("Line number:~p~n", [?LINE]),
                                    case erl_parse:parse_exprs(ErlTokens) of
                                        {ok, ErlAbsForm} ->
                                            ?INFO("Line number:~p~n", [?LINE]),
                                            case erl_eval:exprs(ErlAbsForm, []) of
                                                {value, Return, _} ->
                                                    ?INFO("Line number:~p~n", [?LINE]),
                                                    {ok, Return, Map};
                                                _ ->
                                                    log_entry(?LINE, "get_increment_result|erl_eval:exprs", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "erl_eval failed"),
                                                    ?ERROR
                                            end;
                                        _ ->
                                            log_entry(?LINE, "get_increment_result|erl_parse:parse_exprs", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "erl_eval failed"),
                                            ?ERROR
                                    end;
                                _ ->
                                    log_entry(?LINE, "get_increment_result|erl_scan:string", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "erl_eval failed"),
                                    ?ERROR
                            end;
                        _ ->
                            log_entry(?LINE, "get_increment_result|get_condition_rule", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "param value failed"),
                            ?ERROR
                    end;
                _ ->
                    log_entry(?LINE, "get_increment_result|generate_query", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "generate_query error"),
                    ?ERROR
            end
    end.

slab_points(_, undefined, Total) ->
    log_entry(?LINE, "slab_points", lists:concat(["ParamValue:undefined|Total:", Total]), "param value undefined"),
    ?ERROR;

slab_points([[RuleID, Amount, _] | Rest], ParamValue, Total) ->
    ?INFO("RuleID:~p|Amount:~p~n", [RuleID, Amount]),
    ?INFO("ParamValue:~p~n", [ParamValue]),
    case get_min_max(RuleID) of
        {Min, infinity} ->
            ?INFO("Min:~p~n", [Min]),
            ?INFO("Line number:~p~n", [?LINE]),
            if
                (ParamValue > Min) or (ParamValue == Min) ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    NewTotal = Total + ((ParamValue - Min) / Amount);
                true ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    NewTotal = Total
            end;
        {Min, Max} ->
            if
                (ParamValue > Max) or (ParamValue == Max) ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    NewTotal = Total + ((Max - Min) / Amount);
                (ParamValue < Max) and (ParamValue > Min) ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    NewTotal = Total + ((ParamValue - Min) / Amount);
                true ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    NewTotal = Total
            end;
        _ ->
            ?INFO("Line number:~p~n", [?LINE]),
            NewTotal = Total
        % ?INFO("NewTotal:~p~n",[NewTotal])
    end,
    slab_points(Rest, ParamValue, NewTotal);

slab_points([], _ParamValue, Total) ->
    ?INFO("Line number:~p~n", [?LINE]),
    ?INFO("Total:~p~n", [Total]),
    {ok, Total}.

graduate_points(_, undefined, Total) ->
    ?INFO("Line number:~p~n", [?LINE]),
    log_entry(?LINE, "graduate_points", lists:concat(["ParamValue:undefined|Total:", Total]), "param value undefined"),
    ?ERROR;

graduate_points([[RuleID, Amount, _] | Rest], ParamValue, Total) ->
    case get_min_max(RuleID) of
        {Min, infinity} ->
            if
                (ParamValue > Min) or (ParamValue == Min) ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    NewTotal = Total + ((ParamValue - Min) * (Amount / 100));
                true ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    NewTotal = Total
            end;
        {Min, Max} ->
            if
                (ParamValue > Max) or (ParamValue == Max) ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    NewTotal = Total + ((Max - Min) * (Amount / 100));
                (ParamValue < Max) and (ParamValue > Min) ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    NewTotal = Total + ((ParamValue - Min) * (Amount / 100));
                true ->
                    ?INFO("Line number:~p~n", [?LINE]),
                    NewTotal = Total
            end;
        _ ->
            ?INFO("Line number:~p~n", [?LINE]),
            NewTotal = Total
    end,
    graduate_points(Rest, ParamValue, NewTotal);

graduate_points([], _ParamValue, Total) ->
    ?INFO("Line number:~p~n", [?LINE]),
    ?INFO("Total:~p~n", [Total]),
    {ok, Total}.

value_points(_, undefined, Total) ->
    ?INFO("Line number:~p~n", [?LINE]),
    log_entry(?LINE, "graduate_points", lists:concat(["ParamValue:undefined|Total:", Total]), "param value undefined"),
    ?ERROR;

value_points([[_, Amount, AmountType] | Rest], ParamValue, Total) ->
    case AmountType of
        0 ->
            ?INFO("Line number:~p~n", [?LINE]),
            NewTotal = Total + Amount;
        1 ->
            ?INFO("Line number:~p~n", [?LINE]),
            NewTotal = Total + (ParamValue * (Amount / 100));
        2 ->
            ?INFO("Line number:~p~n", [?LINE]),
            NewTotal = Total + (ParamValue / Amount)
    end,
    value_points(Rest, ParamValue, NewTotal);

value_points([], _ParamValue, Total) ->
    ?INFO("Line number:~p~n", [?LINE]),
    {ok, Total}.

get_condition_values([Head | Rest], Pair) ->
    case Head of
        {integer, _, Value} ->
            ?INFO("Line number:~p~n", [?LINE]),
            get_condition_values(Rest, lists:append(Pair, [Value]));
        _ ->
            ?INFO("Line number:~p~n", [?LINE]),
            get_condition_values(Rest, Pair)
    end;

get_condition_values([], Pair) ->
    ?INFO("Line number:~p~n", [?LINE]),
    ?INFO("Pair:~p~n", [Pair]),
    Pair.

get_min_max(RuleID) ->
    SqlQuery = lists:concat(["select \"finalSet\" from \"Rule\" where \"Rule\".\"id\" = ", RuleID]),
    ?INFO("get_min_max|SqlQuery:~p~n", [SqlQuery]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_min_max|execute_sql", lists:concat(["RuleID:", RuleID]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "get_min_max|execute_sql", lists:concat(["RuleID:", RuleID]), ?SQL_EMPTY),
            ?ERROR;
        {ok, [[FinalSet] | _]} ->
            ?INFO("Line number:~p~n", [?LINE]),
            {ok, ErlTokens, _} = erl_scan:string(lists:concat(["Result=", FinalSet, "."])),
            case get_condition_values(ErlTokens, []) of
                [Min, Max] ->
                    ?INFO("{Min,Max}:~p~n", [{Min, Max}]),
                    {Min, Max};
                [Min] ->
                    ?INFO("{Min,Max}:~p~n", [{Min, infinity}]),
                    {Min, infinity}
            end
    end.

%%%------------------------------------common functions--------------------------------------
round(Number, Precision) ->
    P = math:pow(10, Precision),
    round(Number * P) / P.

% evaluate the rule condition in the rule table.
get_optin_customer(RuleID, MSISDN) ->
    SqlQuery = lists:concat(["select \"finalSet\" from \"Rule\" where \"Rule\".\"id\" = ", RuleID]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_optin_customer|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "get_optin_customer|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), ?SQL_EMPTY),
            ?ERROR;
        {ok, [[undefined] | _]} ->
            log_entry(?LINE, "get_optin_customer|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "finalSet undefined"),
            ?ERROR;
        {ok, [[Condition] | _]} ->
            ?INFO("Condition:~p~n", [Condition]),
            case generate_query(RuleID, MSISDN) of
                {ok, ResultMap} ->
                    case get_condition_rule(Condition, ResultMap) of
                        {ok, NewCondition} ->
                            case erl_scan:string(lists:concat(["Result=", NewCondition, "."])) of
                                {ok, ErlTokens, _} ->
                                    case erl_parse:parse_exprs(ErlTokens) of
                                        {ok, ErlAbsForm} ->
                                            case erl_eval:exprs(ErlAbsForm, []) of
                                                {value, Return, _} ->
                                                    {ok, Return};
                                                _ ->
                                                    log_entry(?LINE, "get_optin_customer| erl_eval:exprs", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "error"),
                                                    ?ERROR
                                            end;
                                        _ ->
                                            log_entry(?LINE, "get_optin_customer|erl_parse:parse_exprs", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "error"),
                                            ?ERROR
                                    end;
                                _ ->
                                    log_entry(?LINE, "get_optin_customer|erl_scan:string", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "error"),
                                    ?ERROR
                            end;
                        _ ->
                            log_entry(?LINE, "get_optin_customer|get_condition_rule", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "error"),
                            ?ERROR
                    end;
                _ ->
                    log_entry(?LINE, "get_optin_customer|generate_query", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "error"),
                    ?ERROR
            end
    end.

% get_condition_result(RuleID,MSISDN)->
%     SqlQuery=lists:concat(["select \"finalSet\" from \"Rule\" where \"Rule\".\"id\" = ",RuleID]),
%     case execute_sql(SqlQuery) of
%         {error,_Error} ->
%             % insert_log_record(#log_record{function=lists:concat(["get_condition_result|execute_sql|Line:",?LINE]),inputs=lists:flatten(SqlQuery),output=lists:flatten(Error),result=0}),
%             ?INFO("Rule Engine error at line:~p~n",[?LINE]),
%             ?ERROR;
%         {ok,[]} ->
%             % insert_log_record(#log_record{function=lists:concat(["get_condition_result|execute_sql|Line:",?LINE]),inputs=lists:flatten(SqlQuery),output="empty",result=0}),
%             ?INFO("Rule Engine error at line:~p~n",[?LINE]),
%             ?ERROR;
%         {ok,[[undefined]]} ->
%             % insert_log_record(#log_record{function=lists:concat(["get_condition_result|execute_sql|Line:",?LINE]),inputs=lists:flatten(SqlQuery),output="condition undefined",result=0}),
%             ?INFO("Rule Engine error at line:~p~n",[?LINE]),
%             ?ERROR;
%         {ok,[[Condition]|_ ]} ->
%             ?INFO("Condition:~p~n",[Condition]),
%             case generate_query(RuleID,MSISDN) of
%                 {ok,ResultMap}->
%                     ?INFO("ResultMap:~p~n",[ResultMap]),
%                     case get_condition_rule(Condition,ResultMap) of
%                         {ok,NewCondition}->
%                             ?INFO("NewCondition:~p~n",[NewCondition]),
%                             case erl_scan:string(lists:concat(["Result=",NewCondition,"."])) of
%                                 {ok,ErlTokens,_}->
%                                     % ?INFO("ErlTokens:~p~n",[ErlTokens]),
%                                     case erl_parse:parse_exprs(ErlTokens) of
%                                         {ok,ErlAbsForm}->
%                                             % ?INFO("ErlAbsForm:~p~n",[ErlAbsForm]),
%                                             % ?INFO("Result:~p~n",[erl_eval:exprs(ErlAbsForm,[])]),
%                                             case erl_eval:exprs(ErlAbsForm,[]) of
%                                                 {value,Return,_ }->
%                                                     ?INFO("Return:~p~n",[Return]),
%                                                     {ok,Return};
%                                                 _ ->
%                                                     ?INFO("line:~p~n",[?LINE]),
%                                                     ?ERROR
%                                             end;
%                                         _ ->
%                                             ?INFO("line:~p~n",[?LINE]),
%                                             ?ERROR
%                                     end;
%                                 _ ->
%                                     ?INFO("line:~p~n",[?LINE]),
%                                     ?ERROR
%                             end;
%                         _ ->
%                             ?INFO("line:~p~n",[?LINE]),
%                             ?ERROR
%                     end;
%                 _->
%                     ?INFO("line:~p~n",[?LINE]),
%                     ?ERROR
%             end
%     end.

get_condition_result(RuleID, MSISDN) ->
    ?INFO("get_condition_result|inputs:~p~n", [{RuleID, MSISDN}]),
    case is_list(MSISDN) of
        true ->
            MSISDNInt = list_to_integer(MSISDN),
            ?INFO("MSISDNInt:~p~n", [MSISDNInt]);
        _ ->
            MSISDNInt = MSISDN,
            ?INFO("MSISDNInt:~p~n", [MSISDNInt])
    end,
    case get_rule_sql(RuleID) of
        {ok, undefined} ->
            log_entry(?LINE, "get_condition_result|get_rule_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "query undefined"),
            ?INFO("line:~p~n", [?LINE]),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "get_condition_result|get_rule_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "query empty"),
            ?INFO("line:~p~n", [?LINE]),
            ?ERROR;
        {ok, SqlQuery} ->
            ?INFO("line:~p~n", [?LINE]),
            CompleteQuery = lists:concat([SqlQuery, " and cx_table.\"msisdn\"='", MSISDN, "'"]),
            ?INFO("CompleteQuery:~p~n", [CompleteQuery]),
            case execute_sql(CompleteQuery) of
                {error, Error} ->
                    log_entry(?LINE, "get_condition_result|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), lists:concat([?SQL_ERROR, ":", Error])),
                    ?ERROR;
                {ok, []} ->
                    log_entry(?LINE, "get_condition_result|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), ?SQL_EMPTY),
                    {ok, false};
                {ok, [[MSISDN] | _]} -> %%{ok,[[8801879993768]]}
                    ?INFO("MSISDN:~p~n", [MSISDN]),
                    {ok, true};
                {ok, [[MSISDNInt] | _]} -> %%{ok,[[8801879993768]]}
                    ?INFO("MSISDNInt:~p~n", [MSISDNInt]),
                    {ok, true};
                {ok, [[Other] | _]} ->
                    log_entry(?LINE, "get_condition_result|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), lists:concat(["Other:", Other])),
                    ?INFO("Other:~p~n", [Other]),
                    {ok, false};
                _ ->
                    log_entry(?LINE, "get_condition_result|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "error"),
                    ?INFO("line:~p~n", [?LINE]),
                    ?ERROR
            end;
        _ ->
            log_entry(?LINE, "get_condition_result", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "error"),
            ?INFO("line:~p~n", [?LINE]),
            ?ERROR
    end.


% getting strings for sql queries from 'RuleVariableSet' table.
generate_query(RuleID, MSISDN) ->
    ?INFO("generate_query:~p~n", [{RuleID, MSISDN}]),
    SqlQuery = lists:concat(["select * from \"RuleVariableSet\" where \"RuleVariableSet\".\"ruleId\" = ", RuleID]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "generate_query|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "generate_query|execute_sql", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), ?SQL_EMPTY),
            ?ERROR;
        {ok, List} ->
            case generate_query_set(List, MSISDN, []) of
                {ok, QuerySet} ->
                    case generate_sql_query(QuerySet, [], []) of
                        {ok, {SqlSet, ParamsSet}} ->
                            ?INFO("SqlSet:~p~n", [SqlSet]),
                            ?INFO("ParamsSet:~p~n", [ParamsSet]),
                            CompleteQuery = lists:concat(["select ", ParamsSet, " from ", SqlSet]),
                            ?INFO("CompleteQuery:~p~n", [CompleteQuery]),
                            case run_sql_query(CompleteQuery) of
                                {ok, [ResultList]} ->
                                    ?INFO("ResultList:~p~n", [ResultList]),
                                    case generate_result_set(ResultList, string:tokens(ParamsSet, ","), []) of
                                        {ok, ResultMap} ->
                                            ?INFO("ResultMap:~p~n", [ResultMap]),
                                            {ok, ResultMap};
                                        _ ->
                                            log_entry(?LINE, "generate_query|generate_result_set", lists:concat(["ResultList:", ResultList, "|ParamsSet:", ParamsSet]), "error"),
                                            ?ERROR
                                    end;
                                _ ->
                                    log_entry(?LINE, "generate_query|run_sql_query", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "error"),
                                    ?ERROR
                            end;
                        _ ->
                            log_entry(?LINE, "generate_query|generate_sql_query", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "error"),
                            ?ERROR
                    end;
                _ ->
                    log_entry(?LINE, "generate_query|generate_query_set", lists:concat(["RuleID:", RuleID, "|MSISDN:", MSISDN]), "error"),
                    ?ERROR
            end
    end.

% generate sql queries for rows of RuleVariableSet table.
generate_query_set([Head | Tail], MSISDN, QuerySet) ->
    ?INFO("generate_query_set|Head:~p~n", [Head]),
    case Head of
        [_, _, FieldName, PramName, "VAL", undefined, undefined, undefined] ->
            Query = lists:concat(["(select \"", PramName, "\" as \"", FieldName, "\" from \"Customer\" where \"msisdn\"=", MSISDN, ")"]),
            Param = lists:concat(["\"", FieldName, "\""]),
            NewQuerySet = lists:append(QuerySet, [{Query, Param}]);

        [_, _, FieldName, PramName, "VAL", "DayPeriod", GapStart, undefined] ->
            Query = lists:concat(["(select \"CustomerDailyDetails\".\"value\" as \"", FieldName, "\" from \"CustomerDailyDetails\" where \"parameter\"='", PramName, "' and \"CustomerDailyDetails\".\"actDate\"= DATE '", GapStart, "' and \"CustomerDailyDetails\".\"msisdn\"='", MSISDN, "')"]),
            Param = lists:concat(["\"", FieldName, "\""]),
            NewQuerySet = lists:append(QuerySet, [{Query, Param}]);

        [_, _, FieldName, PramName, "VAL", "WeekPeriod", GapStart, undefined] ->
            Query = lists:concat(["(select \"CustomerWeeklyDetails\".\"value\" as \"", FieldName, "\" from \"CustomerWeeklyDetails\" where \"parameter\"='", PramName, "' and \"CustomerWeeklyDetails\".\"actDate\"= DATE '", GapStart, "' and \"CustomerWeeklyDetails\".\"msisdn\"='", MSISDN, "')"]),
            Param = lists:concat(["\"", FieldName, "\""]),
            NewQuerySet = lists:append(QuerySet, [{Query, Param}]);

        [_, _, FieldName, PramName, "VAL", "MonthPeriod", GapStart, undefined] ->
            Query = lists:concat(["(select \"CustomerMonthlyDetails\".\"value\" as \"", FieldName, "\" from \"CustomerMonthlyDetails\" where \"parameter\"='", PramName, "' and \"CustomerMonthlyDetails\".\"actMonth\"= DATE '", GapStart, "' and \"CustomerMonthlyDetails\".\"msisdn\"='", MSISDN, "')"]),
            Param = lists:concat(["\"", FieldName, "\""]),
            NewQuerySet = lists:append(QuerySet, [{Query, Param}]);

        [_, _, FieldName, PramName, "SUM", "LastDays", Count, undefined] ->
            Query = lists:concat(["(select SUM(\"value\") as \"", FieldName, "\" from \"CustomerDailyDetails\" where \"parameter\"='", PramName, "' and trunc(SYSDATE)>\"CustomerDailyDetails\".\"actDate\" and trunc(SYSDATE)-", Count, "<=\"CustomerDailyDetails\".\"actDate\" and \"CustomerDailyDetails\".\"msisdn\"='", MSISDN, "')"]),
            Param = lists:concat(["\"", FieldName, "\""]),
            NewQuerySet = lists:append(QuerySet, [{Query, Param}]);

        [_, _, FieldName, PramName, "AVG", "LastDays", Count, undefined] ->
            Query = lists:concat(["(select SUM(\"value\")/", Count, " as \"", FieldName, "\" from \"CustomerDailyDetails\" where \"parameter\"='", PramName, "' and trunc(SYSDATE)>\"CustomerDailyDetails\".\"actDate\" and trunc(SYSDATE)-", Count, "<=\"CustomerDailyDetails\".\"actDate\" and \"CustomerDailyDetails\".\"msisdn\"='", MSISDN, "')"]),
            Param = lists:concat(["\"", FieldName, "\""]),
            NewQuerySet = lists:append(QuerySet, [{Query, Param}]);

        [_, _, FieldName, PramName, "SUM", "LastWeeks", Count, undefined] ->
            Query = lists:concat(["(select SUM(\"value\") as \"", FieldName, "\" from \"CustomerWeeklyDetails\" where \"parameter\"='", PramName, "' and trunc(SYSDATE)-", ((calendar:day_of_the_week(date()) rem 7) + 1), ">\"CustomerWeeklyDetails\".\"actDate\" and trunc(SYSDATE)-", ((calendar:day_of_the_week(date()) rem 7) + list_to_integer(Count) * 7), "<=\"CustomerWeeklyDetails\".\"actDate\" and \"CustomerWeeklyDetails\".\"msisdn\"='", MSISDN, "')"]),
            Param = lists:concat(["\"", FieldName, "\""]),
            NewQuerySet = lists:append(QuerySet, [{Query, Param}]);

        [_, _, FieldName, PramName, "AVG", "LastWeeks", Count, undefined] ->
            Query = lists:concat(["(select SUM(\"value\")/", Count, " as \"", FieldName, "\" from \"CustomerWeeklyDetails\" where \"parameter\"='", PramName, "' and trunc(SYSDATE)-", ((calendar:day_of_the_week(date()) rem 7) + 1), ">\"CustomerWeeklyDetails\".\"actDate\" and trunc(SYSDATE)-", ((calendar:day_of_the_week(date()) rem 7) + list_to_integer(Count) * 7), "<=\"CustomerWeeklyDetails\".\"actDate\" and \"CustomerWeeklyDetails\".\"msisdn\"='", MSISDN, "')"]),
            Param = lists:concat(["\"", FieldName, "\""]),
            NewQuerySet = lists:append(QuerySet, [{Query, Param}]);

        [_, _, FieldName, PramName, "SUM", "LastMonths", Count, undefined] ->
            Query = lists:concat(["(select SUM(\"value\") as \"", FieldName, "\" from \"CustomerMonthlyDetails\" where \"parameter\"='", PramName, "' and ADD_MONTHS(trunc(SYSDATE), -1)>\"CustomerMonthlyDetails\".\"actMonth\" and ADD_MONTHS(trunc(SYSDATE), -", Count, "-1)<=\"CustomerMonthlyDetails\".\"actMonth\" and \"CustomerMonthlyDetails\".\"msisdn\"='", MSISDN, "')"]),
            Param = lists:concat(["\"", FieldName, "\""]),
            NewQuerySet = lists:append(QuerySet, [{Query, Param}]);

        [_, _, FieldName, PramName, "AVG", "LastMonths", Count, undefined] ->
            Query = lists:concat(["(select SUM(\"value\")/", Count, " as \"", FieldName, "\" from \"CustomerMonthlyDetails\" where \"parameter\"='", PramName, "' and ADD_MONTHS(trunc(SYSDATE), -1)>\"CustomerMonthlyDetails\".\"actMonth\" and ADD_MONTHS(trunc(SYSDATE), -", Count, "-1)<=\"CustomerMonthlyDetails\".\"actMonth\" and \"CustomerMonthlyDetails\".\"msisdn\"='", MSISDN, "')"]),
            Param = lists:concat(["\"", FieldName, "\""]),
            NewQuerySet = lists:append(QuerySet, [{Query, Param}]);

        [_, _, FieldName, PramName, "SUM", "DayPeriod", GapStart, GapEnd] ->
            Query = lists:concat(["(select SUM(\"value\") as \"", FieldName, "\" from \"CustomerDailyDetails\" where \"parameter\"='", PramName, "' and DATE '", GapStart, "'<=\"CustomerDailyDetails\".\"actDate\" and DATE '", GapEnd, "'>=\"CustomerDailyDetails\".\"actDate\" and \"CustomerDailyDetails\".\"msisdn\"='", MSISDN, "')"]),
            Param = lists:concat(["\"", FieldName, "\""]),
            NewQuerySet = lists:append(QuerySet, [{Query, Param}]);

        [_, _, FieldName, PramName, "AVG", "DayPeriod", GapStart, GapEnd] ->
            Query = lists:concat(["(select SUM(\"value\")/((DATE '", GapEnd, "'-DATE '", GapStart, "')+1) as \"", FieldName, "\" from \"CustomerDailyDetails\" where \"parameter\"='", PramName, "' and DATE '", GapStart, "'<=\"CustomerDailyDetails\".\"actDate\" and DATE '", GapEnd, "'>=\"CustomerDailyDetails\".\"actDate\" and \"CustomerDailyDetails\".\"msisdn\"='", MSISDN, "')"]),
            Param = lists:concat(["\"", FieldName, "\""]),
            NewQuerySet = lists:append(QuerySet, [{Query, Param}]);

        [_, _, FieldName, PramName, "SUM", "WeekPeriod", GapStart, GapEnd] ->
            Query = lists:concat(["(select SUM(\"value\") as \"", FieldName, "\" from \"CustomerWeeklyDetails\" where \"parameter\"='", PramName, "' and DATE '", GapStart, "'<=\"CustomerWeeklyDetails\".\"actWeek\" and DATE '", GapEnd, "'>=\"CustomerWeeklyDetails\".\"actWeek\" and \"CustomerWeeklyDetails\".\"msisdn\"='", MSISDN, "')"]),
            Param = lists:concat(["\"", FieldName, "\""]),
            NewQuerySet = lists:append(QuerySet, [{Query, Param}]);

        [_, _, FieldName, PramName, "AVG", "WeekPeriod", GapStart, GapEnd] ->
            Query = lists:concat(["(select SUM(\"value\")/((DATE '", GapEnd, "'-DATE '", GapStart, "')+1) as \"", FieldName, "\" from \"CustomerWeeklyDetails\" where \"parameter\"='", PramName, "' and DATE '", GapStart, "'<=\"CustomerWeeklyDetails\".\"actWeek\" and DATE '", GapEnd, "'>=\"CustomerWeeklyDetails\".\"actWeek\" and \"CustomerWeeklyDetails\".\"msisdn\"='", MSISDN, "')"]),
            Param = lists:concat(["\"", FieldName, "\""]),
            NewQuerySet = lists:append(QuerySet, [{Query, Param}]);

        [_, _, FieldName, PramName, "SUM", "MonthPeriod", GapStart, GapEnd] ->
            Query = lists:concat(["(select SUM(\"value\") as \"", FieldName, "\" from \"CustomerMonthlyDetails\" where \"parameter\"='", PramName, "' and DATE '", GapStart, "'<=\"CustomerMonthlyDetails\".\"actMonth\" and DATE '", GapEnd, "'>=\"CustomerMonthlyDetails\".\"actMonth\" and \"CustomerMonthlyDetails\".\"msisdn\"='", MSISDN, "')"]),
            Param = lists:concat(["\"", FieldName, "\""]),
            NewQuerySet = lists:append(QuerySet, [{Query, Param}]);

        [_, _, FieldName, PramName, "AVG", "MonthPeriod", GapStart, GapEnd] ->
            Query = lists:concat(["(select SUM(\"value\")/((DATE '", GapEnd, "'-DATE '", GapStart, "')+1) as \"", FieldName, "\" from \"CustomerMonthlyDetails\" where \"parameter\"='", PramName, "' and DATE '", GapStart, "'<=\"CustomerMonthlyDetails\".\"actMonth\" and DATE '", GapEnd, "'>=\"CustomerMonthlyDetails\".\"actMonth\" and \"CustomerMonthlyDetails\".\"msisdn\"='", MSISDN, "')"]),
            Param = lists:concat(["\"", FieldName, "\""]),
            NewQuerySet = lists:append(QuerySet, [{Query, Param}]);

        _ ->
            NewQuerySet = QuerySet
    end,
    generate_query_set(Tail, MSISDN, NewQuerySet);

generate_query_set([], _MSISDN, QuerySet) ->
    ?INFO("QuerySet:~p~n", [QuerySet]),
    {ok, QuerySet}.

% combining sql queries and param lists seperately.
generate_sql_query([{Query, Param} | Rest], SqlQuerySet, ParamSet) ->
    ?INFO("SqlQuerySet:~p~n", [SqlQuerySet]),
    ?INFO("ParamSet:~p~n", [ParamSet]),
    generate_sql_query(Rest, lists:append(SqlQuerySet, [Query]), lists:append(ParamSet, [Param]));

generate_sql_query([], SqlList, ParamList) ->
    ?INFO("ParamList:~p~n", [ParamList]),
    case {SqlList, ParamList} of
        {[], []} ->
            log_entry(?LINE, "generate_query|generate_query_set", "SqlList,ParamList", "empty,empty"),
            ?ERROR;
        {undefined, undefined} ->
            log_entry(?LINE, "generate_query|generate_query_set", "SqlList,ParamList", "undefined,undefined"),
            ?ERROR;
        {_, []} ->
            log_entry(?LINE, "generate_query|generate_query_set", "SqlList,ParamList", "_,empty"),
            ?ERROR;
        {[], _} ->
            log_entry(?LINE, "generate_query|generate_query_set", "SqlList,ParamList", "empty,_"),
            ?ERROR;
        _ ->
            {ok, {string:join(SqlList, ","), string:join(ParamList, ",")}}
    end.

% execute the complete sql query and return the values for param tags.
run_sql_query(Query) ->
    ?INFO("Query:~p~n", [Query]),
    case execute_sql(Query) of
        {error, Error} ->
            log_entry(?LINE, "run_sql_query|execute_sql", "Query", lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "run_sql_query|execute_sql", "Query", ?SQL_EMPTY),
            ?ERROR;
        {ok, List} ->
            {ok, List}
    end.

% convert_condition(Condition,[[_ ,_ ,VarName,ParamName ,_ ,PeriodType ,_ ,_ ]|Rest])->
%     % NewCondition = re:replace(Condition, "'", "'||chr(39)||'", [global, {return, list}]),
%     ?INFO("VarName:~p~n",[VarName]),
%     case PeriodType of
%         undefined -> %%replace varName of demographic values by paramName.
%             NewCondition = re:replace(Condition,"{"++VarName++"}","\""++ParamName++"\"",[global,{return,list}]);
%         _ ->
%             NewCondition = re:replace(Condition,"{"++VarName++"}","\""++VarName++"\"",[global,{return,list}])
%     end,
%     % NewCondition = re:replace(Condition,"{"++VarName++"}","\""++VarName++"\"",[global,{return,list}]),
%     convert_condition(NewCondition,Rest);

% convert_condition(Condition,[])->
%     NewCondition = re:replace(Condition,"==","=",[global,{return,list}]),
%     {ok,NewCondition}.

% generate condition string by replacing param tags by their values.
get_condition_rule(Condition, [{Tag, Value} | Rest]) ->
    NewCondition = re:replace(Condition, "{" ++ Tag ++ "}", convert_value_to_string(Value), [global, {return, list}]),
    % ?INFO("Converted NewCondition: ~p~n",[NewCondition]),
    get_condition_rule(NewCondition, Rest);

get_condition_rule(Condition, []) ->
    ?INFO("Converted condtion: ~p~n", [Condition]),
    ForErlang1 = re:replace(Condition, "<=", "=<", [global, {return, list}]),
    % ?INFO("ForErlang1:~p~n",[ForErlang1]),
    ForErlang2 = re:replace(ForErlang1, "!=", "/=", [global, {return, list}]),
    % ?INFO("ForErlang2:~p~n",[ForErlang2]),
    {ok, ForErlang2}.

% convert value to string according to their variable types.
% re:replace(Subs_address, "'", "'||chr(39)||'", [global, {return, list}]).
convert_value_to_string(Value) ->
    if
        is_integer(Value) ->
            erlang:integer_to_list(Value);
        is_float(Value) ->
            erlang:float_to_list(Value, [{decimals, 2}]);
        is_list(Value) ->
            % "'"++Value++"'";
            lists:concat(["'", Value, "'"]);
    % list_to_atom(Value);
        is_atom(Value) ->
            erlang:atom_to_list(Value)
    end.

% generate key/value pair from query results set and param set.
generate_result_set([QueryResult | QueryResultRest], [Param | ParamRest], Map) ->
    NewParam = re:replace(Param, "(\")", "", [global, {return, list}]),
    % ?INFO("NewParam:~p~n",[NewParam]),
    NewMap = lists:append(Map, [{NewParam, QueryResult}]),
    % ?INFO("NewMap:~p~n",[NewMap]),
    generate_result_set(QueryResultRest, ParamRest, NewMap);

generate_result_set([], _, FinalMap) ->
    {ok, FinalMap};

generate_result_set(_, [], FinalMap) ->
    {ok, FinalMap};

generate_result_set([], [], FinalMap) ->
    {ok, FinalMap}.

%%%--------------------------------------log function-----------------------------------------
log_entry(Line, Function, Inputs, Output) ->
    if
        (((Line /= []) or (Line /= undefined))
            and ((Function /= []) or (Function /= undefined))
            and ((Inputs /= []) or (Inputs /= undefined))
            and ((Output /= []) or (Output /= undefined))) ->
            Query = lists:concat(["INSERT INTO \"RuleEngineEventLog\" (\"dateTime\", \"line\", \"function\", \"inputs\", \"output\") VALUES (CURRENT_TIMESTAMP, '", Line, "', '", Function, "', '", Inputs, "', '", Output, "')"]),
            case execute_sql(Query) of
                {ok, 1} ->
                    ?LOG_SUCESS;
                Error ->
                    ?INFO("Error:~p~n", Error),
                    ?LOG_ERROR
            end;
        true ->
            ?LOG_EMPTY
    end.
%%%-------------------------------------rule execution----------------------------------------
get_rule_sql([RuleID]) ->
    get_rule_sql(RuleID);

get_rule_sql(RuleID) ->
    ?INFO("RuleID:~p~n", [RuleID]),
    SqlQuery = lists:concat(["select \"finalSet\" from \"Rule\" where \"Rule\".\"id\" = '", RuleID, "'"]),
    ?INFO("SqlQuery:~p~n", [SqlQuery]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            ?INFO("Line:~p~n", [?LINE]),
            log_entry(?LINE, "get_rule_sql|execute_sql", lists:concat(["RuleID:", RuleID]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            ?INFO("Line:~p~n", [?LINE]),
            log_entry(?LINE, "get_rule_sql|execute_sql", lists:concat(["RuleID:", RuleID]), ?SQL_EMPTY),
            ?ERROR;
        {ok, [[Condition] | _]} ->
            ?INFO("Condition:~p~n", [Condition]),
            SqlQuery1 = lists:concat(["select * from \"RuleVariableSet\" where \"RuleVariableSet\".\"ruleId\" = '", RuleID, "'"]),
            case execute_sql(SqlQuery1) of
                {error, Error} ->
                    ?INFO("Line:~p~n", [?LINE]),
                    log_entry(?LINE, "get_rule_sql|execute_sql", lists:concat(["RuleID:", RuleID]), lists:concat([?SQL_ERROR, ":", Error])),
                    ?ERROR;
                {ok, []} ->
                    ?INFO("Line:~p~n", [?LINE]),
                    log_entry(?LINE, "get_rule_sql|execute_sql", lists:concat(["RuleID:", RuleID]), ?SQL_EMPTY),
                    ?ERROR;
                {ok, List} ->
                    Result = get_param_sql(List, [], []),
                    ?INFO("Result:~p~n", [Result]),
                    case Result of
                        {ok, {[], []}} ->
                            log_entry(?LINE, "get_rule_sql|execute_sql", "ParamSQlSet,WhereSet", "empty,empty"),
                            case convert_condition(Condition, List) of
                                {ok, NewCondition} ->
                                    ?INFO("Line number:~p~n", [?LINE]),
                                    case check_special_day(List, []) of
                                        [] ->
                                            ?INFO("Line number:~p~n", [?LINE]),
                                            FinalSQL = lists:concat([?PARENT_SQL1, " where ", "(", NewCondition, ")"]),
                                            ?INFO("FinalSQL:~p~n", [FinalSQL]),
                                            {ok, FinalSQL};
                                        AddList ->
                                            ?INFO("Line number:~p~n", [?LINE]),
                                            FinalSQL = lists:concat([?PARENT_SQL1, " where ", "(", NewCondition, ")"]) ++ AddList,
                                            ?INFO("FinalSQL:~p~n", [FinalSQL]),
                                            {ok, FinalSQL}
                                    end;
                                _ ->
                                    log_entry(?LINE, "get_rule_sql|convert_condition", lists:concat(["Condition:", Condition, "|List:", List]), "error"),
                                    ?ERROR
                            end;
                        {ok, {ParamSQlSet, WhereSet}} ->
                            ?INFO("Line number:~p~n", [?LINE]),
                            MidSql = string:join(ParamSQlSet, ","),
                            WhereSql = string:join(WhereSet, "and"),
                            ?INFO("Line number:~p~n", [?LINE]),
                            case convert_condition(Condition, List) of
                                {ok, NewCondition} ->
                                    ?INFO("Line number:~p~n", [?LINE]),
                                    case check_special_day(List, []) of
                                        [] ->
                                            ?INFO("Line number:~p~n", [?LINE]),
                                            FinalSQL = lists:concat([?PARENT_SQL, MidSql, " where ", WhereSql, " and ", "(", NewCondition, ")"]),
                                            ?INFO("FinalSQL:~p~n", [FinalSQL]),
                                            {ok, FinalSQL};
                                        AddList ->
                                            ?INFO("Line number:~p~n", [?LINE]),
                                            FinalSQL = lists:concat([?PARENT_SQL, MidSql, " where ", WhereSql, " and ", "(", NewCondition, ")"]) ++ AddList,
                                            ?INFO("FinalSQL:~p~n", [FinalSQL]),
                                            {ok, FinalSQL}
                                    end;
                                _ ->
                                    log_entry(?LINE, "get_rule_sql|convert_condition", lists:concat(["Condition:", Condition, "|List:", List]), "error"),
                                    ?ERROR
                            end
                    end
            end
    end.

check_special_day([Head | Rest], AddList) ->
    ?INFO("Line number:~p~n", [?LINE]),
    case Head of
        [_, _, _, ParamName, "DATE", _, _, _] ->
            ?INFO("Line number:~p~n", [?LINE]),
            NewAddList = AddList ++ lists:concat([" and trunc(cx_table.\"", ParamName, "\")= trunc(sysdate)"]),
            ?INFO("NewAddList:~p~n", [NewAddList]),
            check_special_day(Rest, NewAddList);
        _ ->
            ?INFO("Line number:~p~n", [?LINE]),
            NewAddList = AddList,
            ?INFO("NewAddList:~p~n", [NewAddList]),
            check_special_day(Rest, NewAddList)
    end;

check_special_day([], AddList) ->
    AddList.

convert_condition(Condition, [[_, _, VarName, ParamName, _, PeriodType, _, _] | Rest]) ->
    % NewCondition = re:replace(Condition, "'", "'||chr(39)||'", [global, {return, list}]),
    ?INFO("VarName:~p~n", [VarName]),
    case PeriodType of
        undefined -> %%replace varName of demographic values by paramName.
            NewCondition = re:replace(Condition, "{" ++ VarName ++ "}", "\"" ++ ParamName ++ "\"", [global, {return, list}]);
        _ ->
            NewCondition = re:replace(Condition, "{" ++ VarName ++ "}", "\"" ++ VarName ++ "\"", [global, {return, list}])
    end,
    % NewCondition = re:replace(Condition,"{"++VarName++"}","\""++VarName++"\"",[global,{return,list}]),
    convert_condition(NewCondition, Rest);

convert_condition(Condition, []) ->
    NewCondition = re:replace(Condition, "==", "=", [global, {return, list}]),
    {ok, NewCondition}.

get_param_sql([[_, _, VarName, ParamName, ValueType, PeriodType, TimeGapStart, TimeGapEnd] | Rest], SqlList, WhereList) ->
    case ValueType of
        "VAL" ->
            ?INFO("VAL~n", []),
            case PeriodType of
                undefined ->
                    NewSqlList = lists:append(SqlList, []),
                    % Where=["(cx_table.\"msisdn\"=",list_to_atom(VarName++?SQL_TABLE),".\"msisdn\")"],
                    NewWhereList = lists:append(WhereList, []),
                    get_param_sql(Rest, NewSqlList, NewWhereList);
                "DayPeriod" ->
                    case TimeGapStart of
                        undefined ->
                            NewWhereList = lists:append(WhereList, []),
                            NewSqlList = lists:append(SqlList, []),
                            get_param_sql(Rest, NewSqlList, NewWhereList);
                        _ ->
                            Sql = [?CON_SQL1, ParamName, ?CON_SQL2_2, "\"" ++ VarName ++ "\"", ?CON_SQL3, "\"CustomerDailyDetails\"", ?CON_SQL4, " \"actDate\" = DATE '" ++ TimeGapStart ++ "' ", ?CON_SQL7, "'" ++ ParamName ++ "' ", ?CON_SQL6, list_to_atom(VarName ++ ?SQL_TABLE)],
                            ?INFO("Sql:~p~n", [lists:concat(Sql)]),
                            NewSqlList = lists:append(SqlList, [lists:concat(Sql)]),
                            Where = ["(cx_table.\"msisdn\"=", list_to_atom(VarName ++ ?SQL_TABLE), ".\"msisdn\")"],
                            ?INFO("Where:~p~n", [lists:concat(Where)]),
                            NewWhereList = lists:append(WhereList, [lists:concat(Where)]),
                            get_param_sql(Rest, NewSqlList, NewWhereList)
                    end;
                "WeekPeriod" ->
                    case TimeGapStart of
                        undefined ->
                            NewWhereList = lists:append(WhereList, []),
                            NewSqlList = lists:append(SqlList, []),
                            get_param_sql(Rest, NewSqlList, NewWhereList);
                        _ ->
                            Sql = [?CON_SQL1, ParamName, ?CON_SQL2_2, "\"" ++ VarName ++ "\"", ?CON_SQL3, "\"CustomerWeeklyDetails\"", ?CON_SQL4, " \"actWeek\" = DATE '" ++ TimeGapStart ++ "' ", ?CON_SQL7, "'" ++ ParamName ++ "' ", ?CON_SQL6, list_to_atom(VarName ++ ?SQL_TABLE)],
                            ?INFO("Sql:~p~n", [lists:concat(Sql)]),
                            NewSqlList = lists:append(SqlList, [lists:concat(Sql)]),
                            Where = ["(cx_table.\"msisdn\"=", list_to_atom(VarName ++ ?SQL_TABLE), ".\"msisdn\")"],
                            ?INFO("Where:~p~n", [lists:concat(Where)]),
                            NewWhereList = lists:append(WhereList, [lists:concat(Where)]),
                            get_param_sql(Rest, NewSqlList, NewWhereList)
                    end;
                "MonthPeriod" ->
                    case TimeGapStart of
                        undefined ->
                            NewWhereList = lists:append(WhereList, []),
                            NewSqlList = lists:append(SqlList, []),
                            get_param_sql(Rest, NewSqlList, NewWhereList);
                        _ ->
                            Sql = [?CON_SQL1, ParamName, ?CON_SQL2_2, "\"" ++ VarName ++ "\"", ?CON_SQL3, "\"CustomerMonthlyDetails\"", ?CON_SQL4, " \"actMonth\" = DATE '" ++ TimeGapStart ++ "' ", ?CON_SQL7, "'" ++ ParamName ++ "' ", ?CON_SQL6, list_to_atom(VarName ++ ?SQL_TABLE)],
                            ?INFO("Sql:~p~n", [lists:concat(Sql)]),
                            NewSqlList = lists:append(SqlList, [lists:concat(Sql)]),
                            Where = ["(cx_table.\"msisdn\"=", list_to_atom(VarName ++ ?SQL_TABLE), ".\"msisdn\")"],
                            ?INFO("Where:~p~n", [lists:concat(Where)]),
                            NewWhereList = lists:append(WhereList, [lists:concat(Where)]),
                            get_param_sql(Rest, NewSqlList, NewWhereList)
                    end;
                _ ->
                    NewWhereList = lists:append(WhereList, []),
                    NewSqlList = lists:append(SqlList, []),
                    get_param_sql(Rest, NewSqlList, NewWhereList)
            end;
        "AVG" ->
            ?INFO("AVG~n", []),
            case PeriodType of
                "DayPeriod" ->
                    ?INFO("PeriodType:~p~n", [PeriodType]),
                    case TimeGapStart of
                        undefined ->
                            NewWhereList = lists:append(WhereList, []),
                            NewSqlList = lists:append(SqlList, []),
                            get_param_sql(Rest, NewSqlList, NewWhereList);
                        _ ->
                            case TimeGapEnd of
                                undefined ->
                                    NewWhereList = lists:append(WhereList, []),
                                    NewSqlList = lists:append(SqlList, []),
                                    get_param_sql(Rest, NewSqlList, NewWhereList);
                                _ ->
                                    Sql = [?CON_SQL1, "sum(\"value\")/", "(DATE '", TimeGapEnd, "'- DATE '", TimeGapStart, "')+1", " as \"" ++ VarName ++ "\"", ?CON_SQL3, "\"CustomerDailyDetails\"", ?CON_SQL4, " \"actDate\" >= DATE '" ++ TimeGapStart ++ "' ", ?CON_SQL5, " \"actDate\" <= DATE '" ++ TimeGapEnd ++ "' ", ?CON_SQL7, "'" ++ ParamName ++ "' ", ?CON_SQL6, list_to_atom(VarName ++ ?SQL_TABLE)],
                                    ?INFO("Sql:~p~n", [lists:concat(Sql)]),
                                    NewSqlList = lists:append(SqlList, [lists:concat(Sql)]),
                                    Where = ["(cx_table.\"msisdn\"=", list_to_atom(VarName ++ ?SQL_TABLE), ".\"msisdn\")"],
                                    ?INFO("Where:~p~n", [lists:concat(Where)]),
                                    NewWhereList = lists:append(WhereList, [lists:concat(Where)]),
                                    get_param_sql(Rest, NewSqlList, NewWhereList)
                            end
                    end;
                "LastDays" ->
                    ?INFO("PeriodType:~p~n", [PeriodType]),
                    case TimeGapStart of
                        undefined ->
                            NewWhereList = lists:append(WhereList, []),
                            NewSqlList = lists:append(SqlList, []),
                            get_param_sql(Rest, NewSqlList, NewWhereList);
                        _ ->
                            case TimeGapEnd of
                                undefined ->
                                    Sql = [?CON_SQL1, "sum(\"value\")/", TimeGapStart, " as \"" ++ VarName ++ "\"", ?CON_SQL3, "\"CustomerDailyDetails\"", ?CON_SQL4, " \"actDate\" > trunc(SYSDATE)-" ++ TimeGapStart + 1, ?CON_SQL5, " \"actDate\" < trunc(SYSDATE) ", ?CON_SQL7, "'" ++ ParamName ++ "' ", ?CON_SQL6, list_to_atom(VarName ++ ?SQL_TABLE)],
                                    ?INFO("Sql:~p~n", [lists:concat(Sql)]),
                                    NewSqlList = lists:append(SqlList, [lists:concat(Sql)]),
                                    Where = ["(cx_table.\"msisdn\"=", list_to_atom(VarName ++ ?SQL_TABLE), ".\"msisdn\")"],
                                    ?INFO("Where:~p~n", [lists:concat(Where)]),
                                    NewWhereList = lists:append(WhereList, [lists:concat(Where)]),
                                    get_param_sql(Rest, NewSqlList, NewWhereList);
                                _ ->
                                    NewWhereList = lists:append(WhereList, []),
                                    NewSqlList = lists:append(SqlList, []),
                                    get_param_sql(Rest, NewSqlList, NewWhereList)
                            end
                    end;
                "WeekPeriod" ->
                    ?INFO("PeriodType:~p~n", [PeriodType]),
                    case TimeGapStart of
                        undefined ->
                            NewWhereList = lists:append(WhereList, []),
                            NewSqlList = lists:append(SqlList, []),
                            get_param_sql(Rest, NewSqlList, NewWhereList);
                        _ ->
                            case TimeGapEnd of
                                undefined ->
                                    NewWhereList = lists:append(WhereList, []),
                                    NewSqlList = lists:append(SqlList, []),
                                    get_param_sql(Rest, NewSqlList, NewWhereList);
                                _ ->
                                    Sql = [?CON_SQL1, "sum(\"value\")/", "1+(DATE '", TimeGapEnd, "'- DATE '", TimeGapStart, "')/7", " as \"" ++ VarName ++ "\"", ?CON_SQL3, "\"CustomerWeeklyDetails\"", ?CON_SQL4, " \"actWeek\" >= DATE '" ++ TimeGapStart ++ "' ", ?CON_SQL5, " \"actWeek\" <= DATE '" ++ TimeGapEnd ++ "' ", ?CON_SQL7, "'" ++ ParamName ++ "' ", ?CON_SQL6, list_to_atom(VarName ++ ?SQL_TABLE)],
                                    ?INFO("Sql:~p~n", [lists:concat(Sql)]),
                                    NewSqlList = lists:append(SqlList, [lists:concat(Sql)]),
                                    Where = ["(cx_table.\"msisdn\"=", list_to_atom(VarName ++ ?SQL_TABLE), ".\"msisdn\")"],
                                    ?INFO("Where:~p~n", [lists:concat(Where)]),
                                    NewWhereList = lists:append(WhereList, [lists:concat(Where)]),
                                    get_param_sql(Rest, NewSqlList, NewWhereList)
                            end
                    end;
                "LastWeeks" -> %%{(calendar:day_of_the_week({2016,1,20}) rem 7)+1, (calendar:day_of_the_week({2016,1,20}) rem 7)+14}
                    ?INFO("PeriodType:~p~n", [PeriodType]),
                    case TimeGapStart of
                        undefined ->
                            NewWhereList = lists:append(WhereList, []),
                            NewSqlList = lists:append(SqlList, []),
                            get_param_sql(Rest, NewSqlList, NewWhereList);
                        _ ->
                            case TimeGapEnd of
                                undefined ->
                                    % Sql=[?CON_SQL1,"sum(\"value\")/",TimeGapStart," as \""++VarName++"\"",?CON_SQL3,"\"CustomerWeeklyDetails\"",?CON_SQL4," \"actWeek\" > SYSDATE-"++TimeGapStart*7+1,?CON_SQL5," \"actWeek\" < SYSDATE ",?CON_SQL7,"'"++ParamName++"' ",?CON_SQL6,list_to_atom(VarName++?SQL_TABLE)],
                                    Sql = [?CON_SQL1, "sum(\"value\")/", TimeGapStart, " as \"" ++ VarName ++ "\"", ?CON_SQL3, "\"CustomerWeeklyDetails\"", ?CON_SQL4, " \"actWeek\" >= trunc(SYSDATE)-", ((calendar:day_of_the_week(date()) rem 7) + list_to_integer(TimeGapStart) * 7), ?CON_SQL5, " \"actWeek\" < trunc(SYSDATE)-", ((calendar:day_of_the_week(date()) rem 7) + 1), " ", ?CON_SQL7, "'" ++ ParamName ++ "' ", ?CON_SQL6, list_to_atom(VarName ++ ?SQL_TABLE)],
                                    ?INFO("Sql:~p~n", [lists:concat(Sql)]),
                                    NewSqlList = lists:append(SqlList, [lists:concat(Sql)]),
                                    Where = ["(cx_table.\"msisdn\"=", list_to_atom(VarName ++ ?SQL_TABLE), ".\"msisdn\")"],
                                    ?INFO("Where:~p~n", [lists:concat(Where)]),
                                    NewWhereList = lists:append(WhereList, [lists:concat(Where)]),
                                    get_param_sql(Rest, NewSqlList, NewWhereList);
                                _ ->
                                    NewWhereList = lists:append(WhereList, []),
                                    NewSqlList = lists:append(SqlList, []),
                                    get_param_sql(Rest, NewSqlList, NewWhereList)
                            end
                    end;
                "MonthPeriod" ->
                    ?INFO("PeriodType:~p~n", [PeriodType]),
                    case TimeGapStart of
                        undefined ->
                            NewWhereList = lists:append(WhereList, []),
                            NewSqlList = lists:append(SqlList, []),
                            get_param_sql(Rest, NewSqlList, NewWhereList);
                        _ ->
                            case TimeGapEnd of
                                undefined ->
                                    NewWhereList = lists:append(WhereList, []),
                                    NewSqlList = lists:append(SqlList, []),
                                    get_param_sql(Rest, NewSqlList, NewWhereList);
                                _ ->
                                    Sql = [?CON_SQL1, "sum(\"value\")/", "MONTHS_BETWEEN(TO_DATE('", TimeGapEnd, "','YYYY-MM-DD'),TO_DATE('", TimeGapStart, "','YYYY-MM-DD'))+1", " as \"" ++ VarName ++ "\"", ?CON_SQL3, "\"CustomerMonthlyDetails\"", ?CON_SQL4, " \"actMonth\" >= DATE '" ++ TimeGapStart ++ "' ", ?CON_SQL5, " \"actMonth\" <= DATE '" ++ TimeGapEnd ++ "' ", ?CON_SQL7, "'" ++ ParamName ++ "' ", ?CON_SQL6, list_to_atom(VarName ++ ?SQL_TABLE)],
                                    ?INFO("Sql:~p~n", [lists:concat(Sql)]),
                                    NewSqlList = lists:append(SqlList, [lists:concat(Sql)]),
                                    Where = ["(cx_table.\"msisdn\"=", list_to_atom(VarName ++ ?SQL_TABLE), ".\"msisdn\")"],
                                    ?INFO("Where:~p~n", [lists:concat(Where)]),
                                    NewWhereList = lists:append(WhereList, [lists:concat(Where)]),
                                    get_param_sql(Rest, NewSqlList, NewWhereList)
                            end
                    end;
                "LastMonths" ->
                    ?INFO("PeriodType:~p~n", [PeriodType]),
                    case TimeGapStart of
                        undefined ->
                            NewWhereList = lists:append(WhereList, []),
                            NewSqlList = lists:append(SqlList, []),
                            get_param_sql(Rest, NewSqlList, NewWhereList);
                        _ ->
                            case TimeGapEnd of
                                undefined ->
                                    % Sql=[?CON_SQL1,"sum(\"value\")/",TimeGapStart," as \""++VarName++"\"",?CON_SQL3,"\"CustomerMonthlyDetails\"",?CON_SQL4," \"actMonth\" >= SYSDATE-"++TimeGapStart,?CON_SQL5," \"actMonth\" < SYSDATE ",?CON_SQL7,"'"++ParamName++"' ",?CON_SQL6,list_to_atom(VarName++?SQL_TABLE)],
                                    Sql = [?CON_SQL1, "sum(\"value\")/", TimeGapStart, " as \"" ++ VarName ++ "\"", ?CON_SQL3, "\"CustomerMonthlyDetails\"", ?CON_SQL4, " \"actMonth\" > ADD_MONTHS( trunc(SYSDATE), -", TimeGapStart, "-1)", ?CON_SQL5, " \"actMonth\" < ADD_MONTHS(trunc(SYSDATE), -1) ", ?CON_SQL7, "'" ++ ParamName ++ "' ", ?CON_SQL6, list_to_atom(VarName ++ ?SQL_TABLE)],
                                    ?INFO("Sql:~p~n", [lists:concat(Sql)]),
                                    NewSqlList = lists:append(SqlList, [lists:concat(Sql)]),
                                    Where = ["(cx_table.\"msisdn\"=", list_to_atom(VarName ++ ?SQL_TABLE), ".\"msisdn\")"],
                                    ?INFO("Where:~p~n", [lists:concat(Where)]),
                                    NewWhereList = lists:append(WhereList, [lists:concat(Where)]),
                                    get_param_sql(Rest, NewSqlList, NewWhereList);
                                _ ->
                                    NewWhereList = lists:append(WhereList, []),
                                    NewSqlList = lists:append(SqlList, []),
                                    get_param_sql(Rest, NewSqlList, NewWhereList)
                            end
                    end;
                _ ->
                    NewWhereList = lists:append(WhereList, []),
                    NewSqlList = lists:append(SqlList, []),
                    get_param_sql(Rest, NewSqlList, NewWhereList)
            end;
        "SUM" ->
            ?INFO("SUM~n", []),
            case PeriodType of
                "DayPeriod" ->
                    ?INFO("PeriodType:~p~n", [PeriodType]),
                    case TimeGapStart of
                        undefined ->
                            NewWhereList = lists:append(WhereList, []),
                            NewSqlList = lists:append(SqlList, []),
                            get_param_sql(Rest, NewSqlList, NewWhereList);
                        _ ->
                            case TimeGapEnd of
                                undefined ->
                                    NewWhereList = lists:append(WhereList, []),
                                    NewSqlList = lists:append(SqlList, []),
                                    get_param_sql(Rest, NewSqlList, NewWhereList);
                                _ ->
                                    Sql = [?CON_SQL1, "sum", ?CON_SQL2_1, "\"" ++ VarName ++ "\"", ?CON_SQL3, "\"CustomerDailyDetails\"", ?CON_SQL4, " \"actDate\" >= DATE '" ++ TimeGapStart ++ "' ", ?CON_SQL5, " \"actDate\" <= DATE '" ++ TimeGapEnd ++ "' ", ?CON_SQL7, "'" ++ ParamName ++ "' ", ?CON_SQL6, list_to_atom(VarName ++ ?SQL_TABLE)],
                                    ?INFO("Sql:~p~n", [lists:concat(Sql)]),
                                    NewSqlList = lists:append(SqlList, [lists:concat(Sql)]),
                                    Where = ["(cx_table.\"msisdn\"=", list_to_atom(VarName ++ ?SQL_TABLE), ".\"msisdn\")"],
                                    ?INFO("Where:~p~n", [lists:concat(Where)]),
                                    NewWhereList = lists:append(WhereList, [lists:concat(Where)]),
                                    get_param_sql(Rest, NewSqlList, NewWhereList)
                            end
                    end;
                "LastDays" ->
                    ?INFO("PeriodType:~p~n", [PeriodType]),
                    ?INFO("TimeGapStart:~p~n", [TimeGapStart]),
                    case TimeGapStart of
                        undefined ->
                            NewWhereList = lists:append(WhereList, []),
                            NewSqlList = lists:append(SqlList, []),
                            get_param_sql(Rest, NewSqlList, NewWhereList);
                        _ ->
                            case TimeGapEnd of
                                undefined ->
                                    Sql = [?CON_SQL1, "sum", ?CON_SQL2_1, "\"" ++ VarName ++ "\"", ?CON_SQL3, "\"CustomerDailyDetails\"", ?CON_SQL4, " \"actDate\" >= trunc(SYSDATE)-" ++ TimeGapStart, ?CON_SQL5, " \"actDate\" < trunc(SYSDATE) ", ?CON_SQL7, "'" ++ ParamName ++ "' ", ?CON_SQL6, list_to_atom(VarName ++ ?SQL_TABLE)],
                                    ?INFO("Sql:~p~n", [lists:concat(Sql)]),
                                    NewSqlList = lists:append(SqlList, [lists:concat(Sql)]),
                                    Where = ["(cx_table.\"msisdn\"=", list_to_atom(VarName ++ ?SQL_TABLE), ".\"msisdn\")"],
                                    ?INFO("Where:~p~n", [lists:concat(Where)]),
                                    NewWhereList = lists:append(WhereList, [lists:concat(Where)]),
                                    get_param_sql(Rest, NewSqlList, NewWhereList);
                                _ ->
                                    NewWhereList = lists:append(WhereList, []),
                                    NewSqlList = lists:append(SqlList, []),
                                    get_param_sql(Rest, NewSqlList, NewWhereList)
                            end
                    end;
                "WeekPeriod" ->
                    ?INFO("PeriodType:~p~n", [PeriodType]),
                    case TimeGapStart of
                        undefined ->
                            NewWhereList = lists:append(WhereList, []),
                            NewSqlList = lists:append(SqlList, []),
                            get_param_sql(Rest, NewSqlList, NewWhereList);
                        _ ->
                            case TimeGapEnd of
                                undefined ->
                                    NewWhereList = lists:append(WhereList, []),
                                    NewSqlList = lists:append(SqlList, []),
                                    get_param_sql(Rest, NewSqlList, NewWhereList);
                                _ ->
                                    Sql = [?CON_SQL1, "sum", ?CON_SQL2_1, "\"" ++ VarName ++ "\"", ?CON_SQL3, "\"CustomerWeeklyDetails\"", ?CON_SQL4, " \"actWeek\" >= DATE '" ++ TimeGapStart ++ "' ", ?CON_SQL5, " \"actWeek\" <= DATE '" ++ TimeGapEnd ++ "' ", ?CON_SQL7, "'" ++ ParamName ++ "' ", ?CON_SQL6, list_to_atom(VarName ++ ?SQL_TABLE)],
                                    ?INFO("Sql:~p~n", [lists:concat(Sql)]),
                                    NewSqlList = lists:append(SqlList, [lists:concat(Sql)]),
                                    Where = ["(cx_table.\"msisdn\"=", list_to_atom(VarName ++ ?SQL_TABLE), ".\"msisdn\")"],
                                    ?INFO("Where:~p~n", [lists:concat(Where)]),
                                    NewWhereList = lists:append(WhereList, [lists:concat(Where)]),
                                    get_param_sql(Rest, NewSqlList, NewWhereList)
                            end
                    end;
                "LastWeeks" ->
                    ?INFO("PeriodType:~p~n", [PeriodType]),
                    ?INFO("TimeGapStart:~p~n", [TimeGapStart]),
                    case TimeGapStart of
                        undefined ->
                            NewWhereList = lists:append(WhereList, []),
                            NewSqlList = lists:append(SqlList, []),
                            get_param_sql(Rest, NewSqlList, NewWhereList);
                        _ ->
                            case TimeGapEnd of
                                undefined ->
                                    % Sql=[?CON_SQL1,"sum",?CON_SQL2_1,"\""++VarName++"\"",?CON_SQL3,"\"CustomerWeeklyDetails\"",?CON_SQL4," \"actWeek\" >= SYSDATE-"++TimeGapStart*7,?CON_SQL5," \"actWeek\" < SYSDATE ",?CON_SQL7,"'"++ParamName++"' ",?CON_SQL6,list_to_atom(VarName++?SQL_TABLE)],
                                    Sql = [?CON_SQL1, "sum", ?CON_SQL2_1, "\"" ++ VarName ++ "\"", ?CON_SQL3, "\"CustomerWeeklyDetails\"", ?CON_SQL4, " \"actWeek\" >= trunc(SYSDATE)-", ((calendar:day_of_the_week(date()) rem 7) + list_to_integer(TimeGapStart) * 7), ?CON_SQL5, " \"actWeek\" < trunc(SYSDATE)-", ((calendar:day_of_the_week(date()) rem 7) + 1), ?CON_SQL7, "'" ++ ParamName ++ "' ", ?CON_SQL6, list_to_atom(VarName ++ ?SQL_TABLE)],
                                    ?INFO("Sql:~p~n", [lists:concat(Sql)]),
                                    NewSqlList = lists:append(SqlList, [lists:concat(Sql)]),
                                    Where = ["(cx_table.\"msisdn\"=", list_to_atom(VarName ++ ?SQL_TABLE), ".\"msisdn\")"],
                                    ?INFO("Where:~p~n", [lists:concat(Where)]),
                                    NewWhereList = lists:append(WhereList, [lists:concat(Where)]),
                                    get_param_sql(Rest, NewSqlList, NewWhereList);
                                _ ->
                                    NewWhereList = lists:append(WhereList, []),
                                    NewSqlList = lists:append(SqlList, []),
                                    get_param_sql(Rest, NewSqlList, NewWhereList)
                            end
                    end;
                "MonthPeriod" ->
                    ?INFO("PeriodType:~p~n", [PeriodType]),
                    case TimeGapStart of
                        undefined ->
                            NewWhereList = lists:append(WhereList, []),
                            NewSqlList = lists:append(SqlList, []),
                            get_param_sql(Rest, NewSqlList, NewWhereList);
                        _ ->
                            case TimeGapEnd of
                                undefined ->
                                    NewWhereList = lists:append(WhereList, []),
                                    NewSqlList = lists:append(SqlList, []),
                                    get_param_sql(Rest, NewSqlList, NewWhereList);
                                _ ->
                                    Sql = [?CON_SQL1, "sum", ?CON_SQL2_1, "\"" ++ VarName ++ "\"", ?CON_SQL3, "\"CustomerMonthlyDetails\"", ?CON_SQL4, " \"actMonth\" >= DATE '" ++ TimeGapStart ++ "' ", ?CON_SQL5, " \"actMonth\" <= DATE '" ++ TimeGapEnd ++ "' ", ?CON_SQL7, "'" ++ ParamName ++ "' ", ?CON_SQL6, list_to_atom(VarName ++ ?SQL_TABLE)],
                                    ?INFO("Sql:~p~n", [lists:concat(Sql)]),
                                    NewSqlList = lists:append(SqlList, [lists:concat(Sql)]),
                                    Where = ["(cx_table.\"msisdn\"=", list_to_atom(VarName ++ ?SQL_TABLE), ".\"msisdn\")"],
                                    ?INFO("Where:~p~n", [lists:concat(Where)]),
                                    NewWhereList = lists:append(WhereList, [lists:concat(Where)]),
                                    get_param_sql(Rest, NewSqlList, NewWhereList)
                            end
                    end;
                "LastMonths" ->
                    ?INFO("PeriodType:~p~n", [PeriodType]),
                    case TimeGapStart of
                        undefined ->
                            NewWhereList = lists:append(WhereList, []),
                            NewSqlList = lists:append(SqlList, []),
                            get_param_sql(Rest, NewSqlList, NewWhereList);
                        _ ->
                            case TimeGapEnd of
                                undefined ->
                                    % Sql=[?CON_SQL1,"sum",?CON_SQL2_1,"\""++VarName++"\"",?CON_SQL3,"\"CustomerMonthlyDetails\"",?CON_SQL4," \"actMonth\" >= SYSDATE-"++TimeGapStart,?CON_SQL5," \"actMonth\" <= SYSDATE ",?CON_SQL7,"'"++ParamName++"' ",?CON_SQL6,list_to_atom(VarName++?SQL_TABLE)],
                                    Sql = [?CON_SQL1, "sum", ?CON_SQL2_1, "\"" ++ VarName ++ "\"", ?CON_SQL3, "\"CustomerMonthlyDetails\"", ?CON_SQL4, " \"actMonth\" >= ADD_MONTHS( trunc(SYSDATE), -", TimeGapStart, ")", ?CON_SQL5, " \"actMonth\" < ADD_MONTHS(trunc(SYSDATE), -1) ", ?CON_SQL7, "'" ++ ParamName ++ "' ", ?CON_SQL6, list_to_atom(VarName ++ ?SQL_TABLE)],
                                    ?INFO("Sql:~p~n", [lists:concat(Sql)]),
                                    NewSqlList = lists:append(SqlList, [lists:concat(Sql)]),
                                    Where = ["(cx_table.\"msisdn\"=", list_to_atom(VarName ++ ?SQL_TABLE), ".\"msisdn\")"],
                                    ?INFO("Where:~p~n", [lists:concat(Where)]),
                                    NewWhereList = lists:append(WhereList, [lists:concat(Where)]),
                                    get_param_sql(Rest, NewSqlList, NewWhereList);
                                _ ->
                                    NewWhereList = lists:append(WhereList, []),
                                    NewSqlList = lists:append(SqlList, []),
                                    get_param_sql(Rest, NewSqlList, NewWhereList)
                            end
                    end;
                _ ->
                    NewWhereList = lists:append(WhereList, []),
                    NewSqlList = lists:append(SqlList, []),
                    get_param_sql(Rest, NewSqlList, NewWhereList)
            end;
        _ ->
            NewWhereList = lists:append(WhereList, []),
            NewSqlList = lists:append(SqlList, []),
            get_param_sql(Rest, NewSqlList, NewWhereList)
    end;

get_param_sql([], SqlList, WhereList) ->
    % ?INFO("SqlList:~p~n",[SqlList]),
    % ?INFO("WhereList:~p~n",[WhereList]),
    {ok, {SqlList, WhereList}}.

%%%----------------------------------secret code generation----------------------------------
get_secret_code(PartnerId) ->
    random:seed(erlang:now()),
    RandomNum = random:uniform(9) * 10000000 + random:uniform(9999999),
    ?INFO("RandomNum:~p~n", [RandomNum]),
    SqlQuery = lists:concat(["select case when exists (select 1 from \"SecretCode\" where \"partnerId\"='", PartnerId, "' and \"code\"='", RandomNum,
        "' and \"createdDateTime\" BETWEEN TO_DATE('", get_current_date(), "','YYYY-MM-DD') and TO_DATE('", get_current_date(),
        " 23:59:59','YYYY-MM-DD HH24:MI:SS')) then 'true' else 'false'  end as \"rec_exists\" from dual"]),

    ?INFO("SqlQuery:~p~n", [SqlQuery]),

    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_secret_code|execute_sql", lists:concat(["PartnerId:", PartnerId]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "get_secret_code|execute_sql", lists:concat(["PartnerId:", PartnerId]), ?SQL_EMPTY),
            RandomNum;
        {ok, [["true"] | _]} ->
            ?INFO("Code Exsists.~n", []),
            get_secret_code(PartnerId);
        {ok, [["false"] | _]} ->
            ?INFO("Code Not Exsists.~n", []),
            RandomNum
    end.

% generate_random_number(Number,Length)->
%     NumberList = [1,2,3,4,5,6,7,8,9],
%     NumberList1 = [0,1,2,3,4,5,6,7,8,9],
%     if
%         Length==1 ->
%             generate_random_number(lists:merge([lists:nth(random:uniform(8),NumberList)],Number),Length+1);
%         Length<9 ->
%             generate_random_number(lists:merge([lists:nth(random:uniform(8),NumberList1)],Number),Length+1);
%         true ->
%             list_to_integer(lists:concat(lists:reverse(Number)))
%     end.

get_current_date() ->
    {Year, Month, Date} = date(),
    lists:concat([Year, "-", Month, "-", Date]).
% integer_to_list(Year)++"-"++integer_to_list(Month)++"-"++integer_to_list(Date).

%%%--------------------------------reward point enability------------------------------------
reward_point_enability(MSISDN, CategoryId) ->
    ?INFO("MSISDN:~p~n", [MSISDN]),
    ?INFO("CategoryId:~p~n", [CategoryId]),
    SqlQuery = lists:concat(["select \"type\" from \"RewardPointsList\" where \"RewardPointsList\".\"msisdn\" = '", MSISDN, "'"]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_secret_code|execute_sql", lists:concat(["MSISDN:", MSISDN, "|CategoryId:", CategoryId]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            SqlQuery1 = lists:concat(["select \"rewardPoints\" from \"Category\" where \"Category\".\"id\"='", CategoryId, "'"]),
            case execute_sql(SqlQuery1) of
                {error, Error} ->
                    log_entry(?LINE, "reward_point_enability|execute_sql", lists:concat(["MSISDN:", MSISDN, "|CategoryId:", CategoryId]), lists:concat([?SQL_ERROR, ":", Error])),
                    ?ERROR;
                {ok, []} ->
                    log_entry(?LINE, "reward_point_enability|execute_sql", lists:concat(["MSISDN:", MSISDN, "|CategoryId:", CategoryId]), ?SQL_EMPTY),
                    ?ERROR;
                {ok, [[0] | _]} ->
                    ?INFO("RewardPoints not enabled:~p~n", [MSISDN]),
                    false;
                {ok, [[1] | _]} ->
                    ?INFO("RewardPoints enabled:~p~n", [MSISDN]),
                    true
            end;
        {ok, [[1] | _]} ->
            ?INFO("BLack List:~p~n", [MSISDN]),
            false;
        {ok, [[0] | _]} ->
            ?INFO("White List:~p~n", [MSISDN]),
            true
    end.

reward_point_enability_batch(MsisdnList) ->
    Fun0 = fun([M, C, E, RP, _BM], {ML, MCL}) ->
        {ML ++ [integer_to_list(M)], MCL ++ [{integer_to_list(M), C, E, RP}]}
           end,
    {MsisdnListStr, MsisdnCatList} = lists:foldl(Fun0, {[], []}, MsisdnList),
    MsisdnStr = string:join(MsisdnListStr, ","),
    SqlQuery = lists:concat(["select \"msisdn\", \"type\" from \"RewardPointsList\" where \"msisdn\" in (", MsisdnStr, ")"]), %% Blacklist
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "reward_point_enability_batch|execute_sql", lists:concat(["RewardPointsList"]), lists:concat([?SQL_ERROR, ":", Error])),
            Res = ?ERROR;
        {ok, []} ->
            Res = {[], [], MsisdnCatList};
        {ok, BlackLT} ->
            Fun1 = fun([M, T], {TL, FL, OL}) ->
                case T of
                    0 ->
                        case lists:keysearch(M, 1, OL) of
                            {value, {_, Ca, En, _}} ->
                                {TL ++ [{M, Ca, En}], FL, lists:keydelete(M, 1, OL)};
                            false ->
                                {TL, FL, lists:keydelete(M, 1, OL)}
                        end;
                    1 ->
                        case lists:keysearch(M, 1, OL) of
                            {value, {_, Ca, En, _}} ->
                                {TL, FL ++ [M], lists:keydelete(M, 1, OL)};
                            false ->
                                {TL, FL, lists:keydelete(M, 1, OL)}
                        end
                end
                   end,
            Res = lists:foldl(Fun1, {[], [], MsisdnCatList}, BlackLT)
    end,
    case Res of
        ?ERROR -> ?ERROR;
        {TrueList, FalseList, CatList} ->
            Fun2 = fun({M, Ca, En, RP}, {TrL, FlL}) ->
                case RP of
                    0 -> %% disable
                        {TrL, FlL ++ [M]};
                    1 ->
                        {TrL ++ [{M, Ca, En}], FlL}
                end
                   end,
            lists:foldl(Fun2, {TrueList, FalseList}, CatList)
    end.

reward_point_enability(MSISDN) ->
    % ?INFO("MSISDN:~p~n",[MSISDN]),
    SqlQuery = lists:concat(["select \"type\" from \"RewardPointsList\" where \"RewardPointsList\".\"msisdn\" = '", MSISDN, "'"]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_secret_code|execute_sql", lists:concat(["MSISDN:", MSISDN]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            SqlQuery1 = lists:concat(["select \"rewardPoints\" from \"Category\",(select \"categoryId\" from \"Customer\" where \"msisdn\"='", MSISDN, "') cx_category where \"Category\".\"id\"=cx_category.\"categoryId\""]),
            case execute_sql(SqlQuery1) of
                {error, Error} ->
                    log_entry(?LINE, "reward_point_enability|execute_sql", lists:concat(["MSISDN:", MSISDN]), lists:concat([?SQL_ERROR, ":", Error])),
                    ?ERROR;
                {ok, []} ->
                    log_entry(?LINE, "reward_point_enability|execute_sql", lists:concat(["MSISDN:", MSISDN]), ?SQL_EMPTY),
                    ?ERROR;
                {ok, [[0] | _]} ->
                    ?INFO("RewardPoints not enabled:~p~n", [MSISDN]),
                    false;
                {ok, [[1] | _]} ->
                    ?INFO("RewardPoints enabled:~p~n", [MSISDN]),
                    true
            end;
        {ok, [[0] | _]} ->
            ?INFO("White List:~p~n", [MSISDN]),
            true;
        {ok, [[1] | _]} ->
            ?INFO("BLack List:~p~n", [MSISDN]),
            false
    end.

%%%--------------------------------bonus money enability------------------------------------
bonus_money_enability(MSISDN, CategoryId) -> %"type" NUMBER(1) DEFAULT 0 NOT NULL, -- {0 - white-list, 1 - black-list}
    ?INFO("MSISDN:~p~n", [MSISDN]),
    ?INFO("CategoryId:~p~n", [CategoryId]),
    SqlQuery = lists:concat(["select \"type\" from \"BonusMoneyList\" where \"BonusMoneyList\".\"msisdn\" = '", MSISDN, "'"]),
    ?INFO("SqlQuery:~p~n", [SqlQuery]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "bonus_money_enability|execute_sql", lists:concat(["MSISDN:", MSISDN, "|CategoryId:", CategoryId]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            ?INFO("Line:~p~n", [?LINE]),
            SqlQuery1 = lists:concat(["select \"bonusMoney\" from \"Category\" where \"Category\".\"id\"='", CategoryId, "'"]),
            ?INFO("SqlQuery1:~p~n", [SqlQuery1]),
            case execute_sql(SqlQuery1) of
                {error, Error} ->
                    log_entry(?LINE, "bonus_money_enability|execute_sql", lists:concat(["MSISDN:", MSISDN, "|CategoryId:", CategoryId]), lists:concat([?SQL_ERROR, ":", Error])),
                    ?INFO("Line:~p~n", [?LINE]),
                    ?ERROR;
                {ok, []} ->
                    log_entry(?LINE, "bonus_money_enability|execute_sql", lists:concat(["MSISDN:", MSISDN, "|CategoryId:", CategoryId]), ?SQL_EMPTY),
                    ?INFO("Line:~p~n", [?LINE]),
                    ?ERROR;
                {ok, [[0] | _]} ->
                    ?INFO("BonusMoney not enabled:~p~n", [MSISDN]),
                    false;
                {ok, [[1] | _]} ->
                    ?INFO("BonusMoney enabled:~p~n", [MSISDN]),
                    true
            end;
        {ok, [[1] | _]} ->
            ?INFO("BLack List:~p~n", [MSISDN]),
            false;
        {ok, [[0] | _]} ->
            ?INFO("White List:~p~n", [MSISDN]),
            true
    end.

bonus_money_enability_batch(MsisdnList) ->
    Fun0 = fun([M, C, E, _RP, BM], {ML, MCL}) ->
        {ML ++ [integer_to_list(M)], MCL ++ [{integer_to_list(M), C, E, BM}]}
           end,
    {MsisdnListStr, MsisdnCatList} = lists:foldl(Fun0, {[], []}, MsisdnList),
    MsisdnStr = string:join(MsisdnListStr, ","),
    SqlQuery = lists:concat(["select \"msisdn\", \"type\" from \"BonusMoneyList\" where \"msisdn\" in (", MsisdnStr, ")"]), %% Blacklist
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "bonus_money_enability_batch|execute_sql", lists:concat(["BonusMoneyList"]), lists:concat([?SQL_ERROR, ":", Error])),
            Res = ?ERROR;
        {ok, []} ->
            Res = {[], [], MsisdnCatList};
        {ok, BlackLT} ->
            Fun1 = fun([M, T], {TL, FL, OL}) ->
                case T of
                    0 ->
                        case lists:keysearch(M, 1, OL) of
                            {value, {_, Ca, En, _}} ->
                                {TL ++ [{M, Ca, En}], FL, lists:keydelete(M, 1, OL)};
                            false ->
                                {TL, FL, lists:keydelete(M, 1, OL)}
                        end;
                    1 ->
                        case lists:keysearch(M, 1, OL) of
                            {value, {_, Ca, En, _}} ->
                                {TL, FL ++ [M], lists:keydelete(M, 1, OL)};
                            false ->
                                {TL, FL, lists:keydelete(M, 1, OL)}
                        end
                end
                   end,
            Res = lists:foldl(Fun1, {[], [], MsisdnCatList}, BlackLT)
    end,
    case Res of
        ?ERROR -> ?ERROR;
        {TrueList, FalseList, CatList} ->
            Fun2 = fun({M, Ca, En, BM}, {TrL, FlL}) ->
                case BM of
                    0 -> %% disable
                        {TrL, FlL ++ [M]};
                    1 ->
                        {TrL ++ [{M, Ca, En}], FlL}
                end
                   end,
            lists:foldl(Fun2, {TrueList, FalseList}, CatList)
    end.

bonus_money_enability(MSISDN) -> %"type" NUMBER(1) DEFAULT 0 NOT NULL, -- {0 - white-list, 1 - black-list}
    ?INFO("MSISDN:~p~n", [MSISDN]),
    SqlQuery = lists:concat(["select \"type\" from \"BonusMoneyList\" where \"BonusMoneyList\".\"msisdn\" = '", MSISDN, "'"]),
    ?INFO("SqlQuery:~p~n", [SqlQuery]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "bonus_money_enability|execute_sql", lists:concat(["MSISDN:", MSISDN]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            SqlQuery1 = lists:concat(["select \"bonusMoney\" from \"Category\",(select \"categoryId\" from \"Customer\" where \"msisdn\"='", MSISDN, "') cx_category where \"Category\".\"id\"=cx_category.\"categoryId\""]),
            case execute_sql(SqlQuery1) of
                {error, Error} ->
                    log_entry(?LINE, "bonus_money_enability|execute_sql", lists:concat(["MSISDN:", MSISDN]), lists:concat([?SQL_ERROR, ":", Error])),
                    ?ERROR;
                {ok, []} ->
                    log_entry(?LINE, "bonus_money_enability|execute_sql", lists:concat(["MSISDN:", MSISDN]), ?SQL_EMPTY),
                    ?ERROR;
                {ok, [[0] | _]} ->
                    ?INFO("BonusMoney not enabled:~p~n", [MSISDN]),
                    false;
                {ok, [[1] | _]} ->
                    ?INFO("BonusMoney enabled:~p~n", [MSISDN]),
                    true
            end;
        {ok, [[1] | _]} ->
            ?INFO("BLack List:~p~n", [MSISDN]),
            false;
        {ok, [[0] | _]} ->
            ?INFO("White List:~p~n", [MSISDN]),
            true
    end.

%%%-----------------------------------Get Bucket Expiry date-----------------------------------------
get_bucket_expiary_date(BucketId, Msisdn) ->
    % ?INFO("BucketId:~p~n",[BucketId]),
    % ?INFO("Msisdn:~p~n",[Msisdn]),
    SqlQuery = lists:concat(["select \"expiryType\",\"expiryValue\",\"optInTime\" from (select \"optInTime\" from \"Customer\" where \"msisdn\"='", Msisdn, "'),(select \"expiryType\",\"expiryValue\" from \"PointBucket\" where \"id\"=", BucketId, ")"]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_bucket_expiary_date|execute_sql", lists:concat(["Msisdn:", Msisdn, "|BucketId:", BucketId]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "get_bucket_expiary_date|execute_sql", lists:concat(["Msisdn:", Msisdn, "|BucketId:", BucketId]), ?SQL_EMPTY),
            ?ERROR;
        {ok, [[ExpiryType, ExpiryValue, OptInTime] | _]} ->
            case ExpiryType of
                0 ->
                    [Month, Date, Year] = string:tokens(ExpiryValue, "/"),
                    {Y, M, D} = date(),
                    Y1 = list_to_integer(Year),
                    M1 = list_to_integer(Month),
                    D1 = list_to_integer(Date),
                    if
                        Y1 > Y ->
                            {ok, {{Y1, M1, D1}, {0, 0, 0}}};
                    % Y1==Y ->
                    %     if
                    %         (M>M1) ->
                    %             {ok,{{Y,list_to_integer(Month),list_to_integer(Date)},{0,0,0}}};
                    %         (M==M1)and(D>D1) ->
                    %             {ok,{{Y,list_to_integer(Month),list_to_integer(Date)},{0,0,0}}};
                    %         true ->
                    %             {ok,{{Y+1,list_to_integer(Month),list_to_integer(Date)},{0,0,0}}}
                    %     end;
                        true ->
                            if
                                (M > M1) ->
                                    {ok, {{Y, list_to_integer(Month), list_to_integer(Date)}, {0, 0, 0}}};
                                (M == M1) and (D > D1) ->
                                    {ok, {{Y, list_to_integer(Month), list_to_integer(Date)}, {0, 0, 0}}};
                                true ->
                                    {ok, {{Y + 1, list_to_integer(Month), list_to_integer(Date)}, {0, 0, 0}}}
                            end
                    end;
                1 ->
                    case OptInTime of
                        undefined ->
                            log_entry(?LINE, "get_bucket_expiary_date|execute_sql", lists:concat(["ExpiryType:", ExpiryType, "|ExpiryValue:", ExpiryValue, "|OptInTime:", OptInTime]), "OptInTime undefined"),
                            ?ERROR;
                        _ ->
                            SqlQuery1 = lists:concat(["select trunc(MONTHS_BETWEEN(SYSDATE,DATE '", convert_date_oracle(OptInTime), "'))from dual"]),
                            case execute_sql(SqlQuery1) of
                                {error, Error} ->
                                    log_entry(?LINE, "get_bucket_expiary_date|execute_sql", lists:concat(["ExpiryType:", ExpiryType, "|ExpiryValue:", ExpiryValue, "|OptInTime:", OptInTime]), lists:concat([?SQL_ERROR, ":", Error])),
                                    ?ERROR;
                                {ok, []} ->
                                    log_entry(?LINE, "get_bucket_expiary_date|execute_sql", lists:concat(["ExpiryType:", ExpiryType, "|ExpiryValue:", ExpiryValue, "|OptInTime:", OptInTime]), ?SQL_EMPTY),
                                    ?ERROR;
                                {ok, [[MonthsCount] | _]} ->
                                    case catch list_to_integer(ExpiryValue) of
                                        ExPeriod when is_integer(ExPeriod) ->
                                            if
                                                (MonthsCount < ExPeriod) or (MonthsCount == ExPeriod) ->
                                                    SqlQuery2 = lists:concat(["select ADD_MONTHS(DATE '", convert_date_oracle(OptInTime), "',", ExpiryValue, ") from dual"]),
                                                    case execute_sql(SqlQuery2) of
                                                        {error, Error} ->
                                                            log_entry(?LINE, "get_bucket_expiary_date|execute_sql", lists:concat(["ExpiryType:", ExpiryType, "|ExpiryValue:", ExpiryValue, "|OptInTime:", OptInTime]), lists:concat([?SQL_ERROR, ":", Error])),
                                                            ?ERROR;
                                                        {ok, []} ->
                                                            log_entry(?LINE, "get_bucket_expiary_date|execute_sql", lists:concat(["ExpiryType:", ExpiryType, "|ExpiryValue:", ExpiryValue, "|OptInTime:", OptInTime]), ?SQL_EMPTY),
                                                            ?ERROR;
                                                        {ok, [[{datetime, ExpDate}] | _]} ->
                                                            {ok, ExpDate}
                                                    end;
                                                MonthsCount > ExPeriod ->
                                                    Ratio = round(MonthsCount / ExPeriod),
                                                    SqlQuery3 = lists:concat(["select ADD_MONTHS(DATE '", convert_date_oracle(OptInTime), "',", (Ratio * ExPeriod), ") from dual"]),
                                                    case execute_sql(SqlQuery3) of
                                                        {error, Error} ->
                                                            log_entry(?LINE, "get_bucket_expiary_date|execute_sql", lists:concat(["ExpiryType:", ExpiryType, "|ExpiryValue:", ExpiryValue, "|OptInTime:", OptInTime]), lists:concat([?SQL_ERROR, ":", Error])),
                                                            ?ERROR;
                                                        {ok, []} ->
                                                            log_entry(?LINE, "get_bucket_expiary_date|execute_sql", lists:concat(["ExpiryType:", ExpiryType, "|ExpiryValue:", ExpiryValue, "|OptInTime:", OptInTime]), ?SQL_EMPTY),
                                                            ?ERROR;
                                                        {ok, [[{datetime, ExpDate}] | _]} ->
                                                            {ok, ExpDate}
                                                    end
                                            end;
                                        _ ->
                                            log_entry(?LINE, "get_bucket_expiary_date", lists:concat(["ExpiryType:", ExpiryType, "|ExpiryValue:", ExpiryValue, "|OptInTime:", OptInTime]), "is_integer failed"),
                                            ?ERROR
                                    end
                            end
                    end;
                2 ->
                    [Min, _] = string:tokens(ExpiryValue, ","),
                    YearPeriod = list_to_integer(Min),
                    {{Year, _, _}, _} = erlang:localtime(),
                    {ok, {{Year + YearPeriod, 12, 31}, {0, 0, 0}}}
            end
    end.

convert_date_oracle({datetime, {{Year, Month, Date}, _}}) ->
    lists:concat([Year, "-", Month, "-", Date]).

% convert_oracle_date_to_erlang(DateString)->
%     % DateString = "11/23/2015",
%     [Month,Date,Year]=string:tokens(DateString,"/"),
%     {{list_to_integer(Year),list_to_integer(Month),list_to_integer(Date)},{0,0,0}}.

%%%------------------------------------------Redeem points-------------------------------------------
redeem_points(Msisdn, BucketId, RedeemPoints) ->
    SqlQuery = lists:concat(["select \"msisdn\",\"expDate\",\"bucketId\",\"points\" from \"CustomerPointsAll\" where \"msisdn\"= '", Msisdn, "' and \"bucketId\"= '", BucketId, "' ORDER BY \"expDate\" ASC"]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "redeem_points|execute_sql", lists:concat(["Msisdn:", Msisdn, "|BucketId:", BucketId, "|RedeemPoints:", RedeemPoints]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "redeem_points|execute_sql", lists:concat(["Msisdn:", Msisdn, "|BucketId:", BucketId, "|RedeemPoints:", RedeemPoints]), ?SQL_EMPTY),
            ?ERROR;
        {ok, List} ->
            redeem_selection(List, RedeemPoints)
    end.

redeem_selection([[Msisdn, ExpDate, BucketId, Points] | Rest], RedeemPoints) ->
    if
        Points > RedeemPoints ->
            update_redeem_raw(Msisdn, ExpDate, BucketId, Points - RedeemPoints);
        Points == RedeemPoints ->
            update_redeem_raw(Msisdn, ExpDate, BucketId, 0);
        Points < RedeemPoints ->
            case update_redeem_raw(Msisdn, ExpDate, BucketId, 0) of
                {ok, ok} ->
                    redeem_selection(Rest, RedeemPoints - Points);
                _ ->
                    redeem_selection([[Msisdn, ExpDate, BucketId, Points] | Rest], RedeemPoints)
            end
    end;

redeem_selection([], 0) ->
    {ok, ok};

redeem_selection([], _) ->
    log_entry(?LINE, "redeem_selection", "List,RedeemPoints", "[],_"),
    ?ERROR;

redeem_selection(_, 0) ->
    log_entry(?LINE, "redeem_selection", "List,RedeemPoints", "_,0"),
    ?ERROR.

update_redeem_raw(Msisdn, ExpDate, BucketId, Points) ->
    SqlQuery = lists:concat(["UPDATE \"CustomerPointsAll\" SET \"points\" = ", Points, " WHERE \"msisdn\" = '", Msisdn, "' and \"expDate\" = ", oracle_datetime(ExpDate), " and \"bucketId\" = '", BucketId, "'"]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "update_redeem_raw|execute_sql", lists:concat(["Msisdn:", Msisdn, "|ExpDate:", ExpDate, "|BucketId:", BucketId, "|Points:", Points]), lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "update_redeem_raw|execute_sql", lists:concat(["Msisdn:", Msisdn, "|ExpDate:", ExpDate, "|BucketId:", BucketId, "|Points:", Points]), ?SQL_EMPTY),
            ?ERROR;
        {ok, 1} ->
            {ok, ok};
        _ ->
            log_entry(?LINE, "update_redeem_raw|execute_sql", lists:concat(["Msisdn:", Msisdn, "|ExpDate:", ExpDate, "|BucketId:", BucketId, "|Points:", Points]), "no match"),
            ?ERROR
    end.

oracle_datetime(0) ->
    null;
oracle_datetime(DateTime) ->
    case DateTime of
        {datetime, {{YY, MN, DD}, {HH, MM, SS}}} ->
            DT = lists:flatten(io_lib:format("~.4.0w-~.2.0w-~.2.0w ~.2.0w:~.2.0w:~.2.0w", [YY, MN, DD, HH, MM, SS]));
        _Else ->
            DT = "0000-00-00 00:00:00"
    end,
    lists:concat(["TO_DATE('", DT, "', 'YYYY-MM-DD HH24:MI:SS')"]).


%%%------------------------------------------Get top 10 partnes-------------------------------------------
get_top_ten_partners() ->
    SqlQuery = lists:concat(["select \"Partner\".\"name\" from \"Partner\",",
        " (select \"partnerId\",\"offerCount\" from (select count(\"offerId\")as \"offerCount\",\"partnerId\" from \"OfferPurchaseHistory\" where \"result\" = 0 group by \"partnerId\") order by \"offerCount\" desc)\"toplist\"",
        " where \"toplist\".\"partnerId\"=\"Partner\".\"id\" ", " and ROWNUM <= 10"]),
    case execute_sql(SqlQuery) of
        {error, Error} ->
            log_entry(?LINE, "get_top_ten_partners|execute_sql", "no inputs", lists:concat([?SQL_ERROR, ":", Error])),
            ?ERROR;
        {ok, []} ->
            log_entry(?LINE, "get_top_ten_partners|execute_sql", "no inputs", ?SQL_EMPTY),
            [];
        {ok, List} ->
            lists:append(List)
    end.

%%%----------------------------------get next schedule date----------------------------------
% get_next_schedule_date(ExeFrequencyType, ExeFrequencyValue)->
%     if
%         ExeFrequencyType == ?EXE_FREQUENCY_DAILY->
%             get_schedule_date(ExeFrequencyValue);
%         ExeFrequencyType == ?EXE_FREQUENCY_WEEKLY->
%             get_schedule_date(ExeFrequencyValue*7);
%         ExeFrequencyType == ?EXE_FREQUENCY_MONTHLY->
%             get_schedule_month(ExeFrequencyValue);
%         ExeFrequencyType == ?EXE_FREQUENCY_QUATER->
%             get_schedule_month(ExeFrequencyValue*4);
%         ExeFrequencyType == ?EXE_FREQUENCY_HALF_YEARLY->
%             get_schedule_month(ExeFrequencyValue*6);
%         ExeFrequencyType == ?EXE_FREQUENCY_YEARLY->
%             get_schedule_month(ExeFrequencyValue*12)
%     end.

% get_schedule_date(ExeFrequencyValue)->
%     SqlQuery = lists:concat(["SELECT (SYSDATE+",ExeFrequencyValue,")FROM DUAL"]),
%     case execute_sql(SqlQuery) of
%         {error,Error} ->
%             insert_log_record(#log_record{function="get_schedule_date|execute_sql|Line:"++integer_to_list(?LINE),inputs=lists:flatten(SqlQuery),output=lists:flatten(Error),result=0}),
%             ?INFO("Rule Engine error at line:~p~n",[?LINE]),
%             ?ERROR;
%         {ok,[]} ->
%             insert_log_record(#log_record{function="get_schedule_date|execute_sql|Line:"++integer_to_list(?LINE),inputs=lists:flatten(SqlQuery),output="empty",result=0}),
%             ?INFO("Rule Engine error at line:~p~n",[?LINE]),
%             ?ERROR;
%         {ok,[[{datetime,ErlDate}]|_ ]} ->
%             ?INFO("ErlDate:~p~n",[ErlDate]),
%             {ok,ErlDate};
%         _ ->
%             ?INFO("Rule Engine error at line:~p~n",[?LINE]),
%             ?ERROR
%     end.

% get_schedule_month(ExeFrequencyValue)->
%     SqlQuery = lists:concat(["SELECT ADD_MONTHS(SYSDATE,",ExeFrequencyValue,")FROM DUAL"]),
%     case execute_sql(SqlQuery) of
%         {error,Error} ->
%             insert_log_record(#log_record{function="get_schedule_month|execute_sql|Line:"++integer_to_list(?LINE),inputs=lists:flatten(SqlQuery),output=lists:flatten(Error),result=0}),
%             ?INFO("Rule Engine error at line:~p~n",[?LINE]),
%             ?ERROR;
%         {ok,[]} ->
%             insert_log_record(#log_record{function="get_schedule_month|execute_sql|Line:"++integer_to_list(?LINE),inputs=lists:flatten(SqlQuery),output="empty",result=0}),
%             ?INFO("Rule Engine error at line:~p~n",[?LINE]),
%             ?ERROR;
%         {ok,[[{datetime,ErlDate}]|_ ]} ->
%             ?INFO("ErlDate:~p~n",[ErlDate]),
%             {ok,ErlDate};
%         _ ->
%             ?INFO("Rule Engine error at line:~p~n",[?LINE]),
%             ?ERROR
%     end.

execute_sql(Sql) ->
    case catch lms_db:execute_sql(Sql, [{pool, ?LMS_SMS_POOL}]) of
        {'EXIT', Error} ->
            io:fwrite("error:~p~n", [Error]),
            Error;
        Else ->
            Else
    end.
            
