package kd.cus.com.spic.reportdata;

import kd.bos.dataentity.OperateOption;
import kd.bos.dataentity.entity.DynamicObject;
import kd.bos.dataentity.metadata.dynamicobject.DynamicObjectType;
import kd.bos.db.DB;
import kd.bos.db.DBRoute;
import kd.bos.db.ResultSetHandler;
import kd.bos.db.SqlParameter;
import kd.bos.entity.MainEntityType;
import kd.bos.entity.datamodel.events.PropertyChangedArgs;
import kd.bos.entity.property.LongProp;
import kd.bos.entity.property.PKFieldProp;
import kd.bos.entity.property.VarcharProp;
import kd.bos.form.control.Toolbar;
import kd.bos.form.control.events.ItemClickEvent;
import kd.bos.form.plugin.AbstractFormPlugin;
import kd.bos.logging.Log;
import kd.bos.logging.LogFactory;
import kd.bos.orm.query.QCP;
import kd.bos.orm.query.QFilter;
import kd.bos.service.business.datamodel.DynamicFormModelProxy;
import kd.bos.servicehelper.BusinessDataServiceHelper;
import kd.bos.servicehelper.DBServiceHelper;
import kd.bos.servicehelper.MetadataServiceHelper;
import kd.bos.servicehelper.operation.OperationServiceHelper;
import kd.bos.servicehelper.operation.SaveServiceHelper;
import kd.fi.bcm.business.serviceHelper.OlapServiceHelper;
import kd.fi.bcm.business.sql.MDResultSet;
import kd.fi.bcm.business.sql.SQLBuilder;
import kd.fi.bcm.common.Pair;

import javax.wsdl.Types;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.stream.Collectors;

public class ExportSetPlugin extends AbstractFormPlugin {

    private static final String UAT = "cwgx";

    private static final String ORG_LOGO = "spic_mul_org";
    private static final String REPORT_LOGO = "spic_mul_report";
    private static final String CURRENCY_LOGO = "spic_currency";
    private static final String YEAR_LOGO = "spic_mul_year";
    private static final String MONTH_LOGO = "spic_mul_month";
    private static final String TIP_LOGO = "spic_largetextfield";
    private static final String BILL_LOGO = "spic_reports_run_values";

    private static List<DynamicObject> orgListCodes = new ArrayList<>();
    private static List<DynamicObject> reportListCodes = new ArrayList<>();
    private static List<String> currencyListCodes = new ArrayList<>();
    private static List<String> yearListCodes = new ArrayList<>();
    private static List<String> monthListCodes = new ArrayList<>();

    /**
     * 监听
     */
    @Override
    public void registerListener(EventObject e) {
        super.registerListener(e);
        Toolbar repairDataBtnBar = this.getControl("tbmain");
        repairDataBtnBar.addItemClickListener(this);
    }
    /**
     * 下拉菜单更新
     */
    @Override
    public void propertyChanged(PropertyChangedArgs e) {
        String name = e.getProperty().getName();
        if (ORG_LOGO.equalsIgnoreCase(name) || REPORT_LOGO.equalsIgnoreCase(name) || CURRENCY_LOGO.equalsIgnoreCase(name) || YEAR_LOGO.equalsIgnoreCase(name) || MONTH_LOGO.equalsIgnoreCase(name)){
            System.out.println("更新查询数据");
            updateSearchData();
        }
    }

    /**
     * 按钮点击事件
     * @param evt
     */

    @Override
    public void itemClick(ItemClickEvent evt) {
        String itemkey = evt.getItemKey();
        if ("spic_export_btn".equalsIgnoreCase(itemkey)){
            Map<Object, DynamicObject> result = search();
            StringBuffer resLog = new StringBuffer();
            int allnum = result.size();
            List<PairEn> dy_pairs = new ArrayList<>();
            long sucnum = 0;
            long salnum = 0;
            result.entrySet().forEach(m->{
                PairEn dy_pair = exportDimension_values(m.getValue());
                dy_pairs.add(dy_pair);
                resLog.append("============================================================================" +
                        "id：\t" + m.getKey() + "\t编号：\t" + m.getValue().getString("billno") + "\t\r\n" +
                        "成功：\t" + dy_pair.getSuc() + "\t\r\n" +
                        "信息：\t" + dy_pair.getMsg() + "\t\r\n");
            });
            sucnum = dy_pairs.stream().filter(m->m.getSuc()).count();
            salnum = dy_pairs.stream().filter(m->!m.getSuc()).count();
            this.getView().showTipNotification("完成：查询"+ allnum +"条，成功"+sucnum+"条，失败"+salnum+"条");
        }
    }

    /**
     * 更新查询数据
     */
    private void updateSearchData(){
        List<DynamicObject> org = (List<DynamicObject>) this.getModel().getValue(ORG_LOGO);
        List<DynamicObject> report = (List<DynamicObject>) this.getModel().getValue(REPORT_LOGO);
        List<String> currency = Arrays.asList(((String) (null == this.getModel().getValue(CURRENCY_LOGO)?",":this.getModel().getValue(CURRENCY_LOGO))).split(","));
        List<String> year = Arrays.asList(((String) (null == this.getModel().getValue(YEAR_LOGO)?",":this.getModel().getValue(YEAR_LOGO))).split(","));
        List<String> month = Arrays.asList(((String) (null == this.getModel().getValue(MONTH_LOGO)?",":this.getModel().getValue(MONTH_LOGO))).split(","));
        System.out.println("更新查询数据");
        String tips = "";

        tips += "所选组织：\r\n" +
                org.stream().map(i->i.getDynamicObject("fbasedataid")).map(i->i.getString("name") + "(" + i.getString("number") + ")").collect(Collectors.joining(",","[","]")) + "\r\n" +
                "所选报表：\r\n" +
                report.stream().map(i->i.getDynamicObject("fbasedataid")).map(i->i.getString("name") + "(" + i.getString("number") + ")").collect(Collectors.joining(",","[","]")) + "\r\n" +
                "所选币别：\r\n" +
                currency.stream().collect(Collectors.joining(",","[","]")) + "\r\n" +
                "所选年度：\r\n" +
                year.stream().collect(Collectors.joining(",","[","]")) + "\r\n" +
                "所选月度：\r\n" +
                month.stream().collect(Collectors.joining(",","[","]")) + "\r\n";
                this.getModel().setValue(TIP_LOGO,tips);
        System.out.println("更新查询数据");
        orgListCodes = org;
        reportListCodes = report;
        currencyListCodes = currency;
        yearListCodes = year;
        monthListCodes = month;
    }

    private Map<Object, DynamicObject> search(){
        Map<Object, DynamicObject> dys = new HashMap<>();
//        QFilter[] qFilters = new QFilter[5];
//        qFilters[0] = currencyListCodes.size()>0?new QFilter("spic_currency", QCP.in, currencyListCodes):QFilter.isNotNull("spic_currency");
//        qFilters[1] = orgListCodes.size()>0?new QFilter("spic_entity_code", QCP.in, orgListCodes):QFilter.isNotNull("spic_entity_code");
//        qFilters[2] = yearListCodes.size()>0?new QFilter("spic_year",QCP.in,"FY_" + yearListCodes):QFilter.isNotNull("spic_year");
//        qFilters[3] = monthListCodes.size()>0?new QFilter("spic_period",QCP.in,"M_M" + monthListCodes):QFilter.isNotNull("spic_period");
//        qFilters[4] = reportListCodes.size()>0?new QFilter("spic_run_report",QCP.in,reportListCodes):QFilter.isNotNull("spic_run_report");
//        Map<Object, DynamicObject> dys = BusinessDataServiceHelper.loadFromCache(BILL_LOGO, qFilters);
        if (currencyListCodes.size()>0 && orgListCodes.size()>0 && yearListCodes.size()>0 && monthListCodes.size()>0 && reportListCodes.size()>0){
            List<DynamicObject> neworgListCodes = orgListCodes;
            List<DynamicObject> newreportListCodes = reportListCodes;
            List<String> newcurrencyListCodes = currencyListCodes.subList(1, currencyListCodes.size());
            List<String> newyearListCodes = yearListCodes.subList(1, yearListCodes.size());
            List<String> newmonthListCodes = monthListCodes.subList(1, monthListCodes.size());
            DynamicObjectType type = MetadataServiceHelper.getDataEntityType("spic_reports_run_values");
            for (String currency : newcurrencyListCodes) {
                for (String year : newyearListCodes) {
                    for (String month : newmonthListCodes) {
                        for (DynamicObject org : neworgListCodes) {
                            for (DynamicObject report : newreportListCodes) {
                                Map<Class<?>, Object> services = new HashMap<>();
                                DynamicFormModelProxy model = new DynamicFormModelProxy("spic_reports_run_values", UUID.randomUUID().toString(), services);
                                model.createNewData();
                                ((MainEntityType) type).getAppId();
                                PKFieldProp pkProp = (PKFieldProp)type.getPrimaryKey();
                                if (pkProp instanceof LongProp) {
                                    model.getDataEntity().set(pkProp, DBServiceHelper.genGlobalLongId());
                                } else if (pkProp instanceof VarcharProp) {
                                    model.getDataEntity().set(pkProp, DBServiceHelper.genStringId());
                                }
                                DynamicObject this_dy = model.getDataEntity(true);
                                this_dy.set("spic_currency",currency);
                                this_dy.set("spic_year",year);
                                this_dy.set("spic_period",month);
                                this_dy.set("spic_entity_name",org.getDynamicObject("fbasedataid").get("name"));
                                this_dy.set("spic_entity_code",org.getDynamicObject("fbasedataid").get("number"));
                                this_dy.set("spic_run_report",report.getDynamicObject("fbasedataid"));
                                this_dy.set("spic_report__num",report.getDynamicObject("fbasedataid").get("number"));
                                this_dy.set("spic_report__name",report.getDynamicObject("fbasedataid").get("name"));
                                Object pkValue = model.getDataEntity().getPkValue();
                                SaveServiceHelper.save(new DynamicObject[]{this_dy});
                                dys.put(pkValue,this_dy);
                            }
                        }
                    }
                }
            }
        }
        return dys;
    }

    private PairEn exportDimension_values(DynamicObject this_dy) {
        String strpkId = this_dy.getPkValue().toString();
        Log logMonitor = LogFactory.getLog(ImportReportPlugin.class);
        List<DynamicObject> this_dy_entrys = new ArrayList<>();
        List<String> outMsgList = new ArrayList<>();
        // Map<String, Object> excuteInfo = new HashMap<>();
        DynamicObject run_report_dy = this_dy.getDynamicObject("spic_run_report");
        run_report_dy = BusinessDataServiceHelper.loadSingle(run_report_dy.getPkValue(),"spic_transfer_reports");
        // Map<String, Object> excuteInfo = new HashMap<>();
        String modelNum = run_report_dy.getString("spic_cubenumber");
//        String modelNum = "CUBEGJDT004937477539494900738";
        try {
            logMonitor.info("开始查询：" + strpkId + "，modelNum为："+modelNum);
            System.out.println("0 modelNum:" + modelNum);

            System.out.println("1 strpkId:" + strpkId);

            List<SqlParameter> params = new ArrayList<>();

            Object reportOrg= this_dy.get("spic_entity_code");
            logMonitor.info("数据开始查询数据库：" + strpkId + "，reportOrg.toString()："+reportOrg.toString());
            DynamicObject orgs = BusinessDataServiceHelper.loadSingleFromCache("spic_merge_orgs",new QFilter[]{new QFilter("number", QCP.equals,reportOrg.toString())});

            String strSQL ="";
            if(null !=orgs && "true".equalsIgnoreCase(orgs.getString("spic_is_merge")) ) //合并体
            {
                logMonitor.info("数据开始合并单位查询数据库：" + strpkId + "，reportOrg.toString()："+reportOrg.toString());
                strSQL = "SELECT trr.fbillno, " +
                        " trr.fk_spic_entity_code, " +
                        " trr.fk_spic_currency, " +
                        " trr.fk_spic_period, " +
                        " trr.fk_spic_year, " +
                        " trsv.fk_spic_process_dim_ec fk_spic_process_dimension, " +
                        " trsv.fk_spic_audit_dim_ec fk_spic_audit_dimension, " +
                        " trsv.fk_spic_changetyp_dim_ec fk_spic_changetyp_dimensi, " +
                        " trsv.fk_spic_multigaap_dim_ec fk_spic_multigaap_dimensi, " +
                        " trsv.fk_spic_account_dim_ec fk_spic_account_dimension, " +
                        " trsv.fk_spic_scenario_dim_ec fk_spic_scenario_dimensio, " +
                        " trsv.fk_spic_internalc_dim_ec fk_spic_internalc_dimensi, " +
                        " trsv.fk_spic_report_row_ec fk_spic_report_col, " +
                        " trsv.fk_spic_report_col_ec fk_spic_report_row, " +
                        " trsv.fk_spic_c1_dim_ec fk_spic_c1_dimension, " +
                        " trsv.fk_spic_c2_dim_ec fk_spic_c2_dimension, " +
                        " trsv.fk_spic_c3_dim_ec fk_spic_c3_dimension, " +
                        " trsv.fk_spic_c4_dim_ec fk_spic_c4_dimension, " +
                        " trsv.fk_spic_c5_dim_ec fk_spic_c5_dimension, " +
                        " trsv.fk_spic_c6_dim_ec fk_spic_c6_dimension, " +
                        " ttrs.fk_spic_cubenumber " +
                        " FROM "+UAT+"_secd.tk_spic_report_run trr, " +
                        " "+UAT+"_secd.tk_spic_report_set trs, " +
                        " "+UAT+"_secd.tk_spic_report_export_c trsv, " +
                        " "+UAT+"_secd.tk_spic_transfer_report ttrs " +
                        " WHERE trr.fk_spic_run_report = trs.fk_spic_transfer_reports " +
                        " AND ttrs.FID = trr.fk_spic_run_report " +
                        " AND trs.FId = trsv.FId " +
                        " AND trr.FId = ? ";
            }
            else
            {
                logMonitor.info("数据开始单体单位查询数据库：" + strpkId + "，reportOrg.toString()："+reportOrg.toString());
                strSQL = "SELECT trr.fbillno, " +
                        " trr.fk_spic_entity_code, " +
                        " trr.fk_spic_currency, " +
                        " trr.fk_spic_period, " +
                        " trr.fk_spic_year, " +
                        " trsv.fk_spic_process_dimensi_e fk_spic_process_dimension, " +
                        " trsv.fk_spic_audit_dimension_e fk_spic_audit_dimension, " +
                        " trsv.fk_spic_changetyp_dim_e fk_spic_changetyp_dimensi, " +
                        " trsv.fk_spic_multigaap_dim_e fk_spic_multigaap_dimensi, " +
                        " trsv.fk_spic_account_dim_e fk_spic_account_dimension, " +
                        " trsv.fk_spic_scenario_dim_e fk_spic_scenario_dimensio, " +
                        " trsv.fk_spic_internalc_dim_e fk_spic_internalc_dimensi, " +
                        " trsv.fk_spic_report_row_e fk_spic_report_col, " +
                        " trsv.fk_spic_report_col_e fk_spic_report_row, " +
                        " trsv.fk_spic_c1_dim_e fk_spic_c1_dimension, " +
                        " trsv.fk_spic_c2_dim_e fk_spic_c2_dimension, " +
                        " trsv.fk_spic_c3_dim_e fk_spic_c3_dimension, " +
                        " trsv.fk_spic_c4_dim_e fk_spic_c4_dimension, " +
                        " trsv.fk_spic_c5_dim_e fk_spic_c5_dimension, " +
                        " trsv.fk_spic_c6_dim_e fk_spic_c6_dimension, " +
                        " ttrs.fk_spic_cubenumber " +
                        " FROM "+UAT+"_secd.tk_spic_report_run trr, " +
                        " "+UAT+"_secd.tk_spic_report_set trs, " +
                        " "+UAT+"_secd.tk_spic_report_export trsv, " +
                        " "+UAT+"_secd.tk_spic_transfer_report ttrs " +
                        " WHERE trr.fk_spic_run_report = trs.fk_spic_transfer_reports " +
                        " AND ttrs.FID = trr.fk_spic_run_report " +
                        " AND trs.FId = trsv.FId " +
                        " AND trr.FId = ? ";
            }
            System.out.println("2 strpkId:" + strpkId);
            logMonitor.info("开始查询：" + "，strSQL："+strSQL+ ";trr.FId = "+ strpkId);
            params.add(new SqlParameter(":FId", Types.STRING_TYPE, strpkId.toString()));
            System.out.println("2.1 add params strpkId:" + strpkId);
            List<Pair<String[], Object>> saveValPairs = new ArrayList<>();
            ResultSetHandler<List<String>> action = (rs) -> {
                // System.out.println("3 strpkId:"+strpkId);
                List<report_dimension> dimensions = new ArrayList<>(10);
                logMonitor.info("开始查询：" + strpkId + "，rs1：");
                while (rs.next()) {
                    System.out.println("4 strpkId:" + strpkId);
                    String process = rs.getString("fk_spic_process_dimension");
                    String currency = rs.getString("fk_spic_currency");
                    String auditTrail = rs.getString("fk_spic_audit_dimension");
                    String changeType = rs.getString("fk_spic_changetyp_dimensi");
                    String multiGAAP = rs.getString("fk_spic_multigaap_dimensi");
                    String entity = rs.getString("fk_spic_entity_code");
                    String account = rs.getString("fk_spic_account_dimension");
                    String year = "FY"+rs.getString("fk_spic_year");
                    String period = "M_M"+rs.getString("fk_spic_period");
                    String scenario = rs.getString("fk_spic_scenario_dimensio");
                    String internalCompany = rs.getString("fk_spic_internalc_dimensi");
                    String c1 = rs.getString("fk_spic_c1_dimension");
                    String c2 = rs.getString("fk_spic_c2_dimension");
                    String c3 = rs.getString("fk_spic_c3_dimension");
                    String c4 = rs.getString("fk_spic_c4_dimension");
                    String c5 = rs.getString("fk_spic_c5_dimension");
                    String c6 = rs.getString("fk_spic_c6_dimension");
                    String cubenumber = rs.getString("fk_spic_cubenumber");
                    System.out.println("process:" + process);
                    logMonitor.info("开始查询：" + strpkId + "，cubenumber："+cubenumber);


                    SQLBuilder sql = new SQLBuilder(cubenumber);// 传入体系编码,bcm_model的number字段值
                    String[] dims = { "Process", "Currency", "AuditTrail", "ChangeType", "MultiGAAP", "Entity",
                            "Account", "Year", "Period", "Scenario", "InternalCompany", "C1", "C2", "C3", "C4", "C5", "C6" };//
                    sql.addSelectField(dims);// 设置dim1维度的成员过滤，dim001和dim002是维度dim1的成员。
                    sql.addFilter("Process", process);
                    sql.addFilter("Currency", currency);
                    sql.addFilter("AuditTrail", auditTrail);
                    sql.addFilter("ChangeType", changeType);
                    sql.addFilter("Entity", entity);
                    sql.addFilter("MultiGAAP", multiGAAP);
                    sql.addFilter("Account", account);
                    sql.addFilter("Year", year);
                    sql.addFilter("Period", period);
                    sql.addFilter("Scenario", scenario);
                    sql.addFilter("InternalCompany", internalCompany);
                    sql.addFilter("C1", c1);
                    sql.addFilter("C2", c2);
                    sql.addFilter("C3", c3);
                    sql.addFilter("C4", c4);
                    sql.addFilter("C5", c5);
                    sql.addFilter("C6", c6);
                    logMonitor.info("查询参数：" + strpkId + ";Process:"+ process);
                    logMonitor.info("查询参数：" + strpkId + "Currency:"+ currency);
                    logMonitor.info("查询参数：" + strpkId + "AuditTrail:"+ auditTrail);
                    logMonitor.info("查询参数：" + strpkId + "ChangeType:"+ changeType);
                    logMonitor.info("查询参数：" + strpkId + "Entity:"+ entity);
                    logMonitor.info("查询参数：" + strpkId + "MultiGAAP:"+ multiGAAP);
                    logMonitor.info("查询参数：" + strpkId + "Account:"+ account);
                    logMonitor.info("查询参数：" + strpkId + "Year:"+ year);
                    logMonitor.info("查询参数：" + strpkId + "Period:"+ period);
                    logMonitor.info("查询参数：" + strpkId + "Scenario:"+ scenario);
                    logMonitor.info("查询参数：" + strpkId + "InternalCompany:"+ internalCompany);
                    logMonitor.info("查询参数：" + strpkId + "：C1："+ c1);
                    logMonitor.info("查询参数：" + strpkId + "C2:"+ c2);
                    logMonitor.info("查询参数：" + strpkId + "C3:"+ c3);
                    logMonitor.info("查询参数：" + strpkId + "C4:"+ c4);
                    logMonitor.info("开始查询：" + strpkId + "，before：OlapServiceHelper。queryData");
                    MDResultSet mrs = OlapServiceHelper.queryData(sql);
                    while (mrs.next()) {// 遍历数据
                        //插入数据
                        logMonitor.info("begin遍历数据：" + strpkId + "，before：OlapServiceHelper。queryData");
                        String fk_spic_report_row =  rs.getString("fk_spic_report_row");
                        String fk_spic_report_col =  rs.getString("fk_spic_report_col");
                        String FMONEY =  mrs.getBigDecimal("FMONEY").toString();
                        logMonitor.info("遍历数据：" + strpkId + "，fk_spic_report_row:"+fk_spic_report_row);
                        logMonitor.info("遍历数据：" + strpkId + "，fk_spic_report_col:"+fk_spic_report_col);
                        logMonitor.info("遍历数据：" + strpkId + "，FMONEY:"+FMONEY);
                        DynamicObject this_dy_entry = new DynamicObject(this_dy.getDynamicObjectCollection("spic_entryentity").getDynamicObjectType());
                        this_dy_entry.set("spic_row_num",fk_spic_report_row);
                        this_dy_entry.set("spic_col_num",fk_spic_report_col);
                        this_dy_entry.set("spic_value",FMONEY);
                        logMonitor.info("begin遍历数据：" + strpkId + "，before：createNewEntryRow");
                        this_dy_entrys.add(this_dy_entry);
                        outMsgList.add("spic_row_num:"  +  fk_spic_report_row + "\t" + "spic_col_num" + fk_spic_report_col + "\t" + "spic_value" + FMONEY + "\t\r\n");
                        logMonitor.info("begin遍历数据：" + strpkId + "，after：createNewEntryRow");
                    }
                    // excuteInfo.put("success", true);
                    // excuteInfo.put("message", "success");
                }
                return null;
            };
            System.out.println("5 strSQL:" + strSQL);
            List<String> rowId = DB.query(DBRoute.meta, strSQL, params.toArray(new SqlParameter[params.size()]),
                    action);
            System.out.println("6 strpkId:" + strpkId);
            this_dy.getDynamicObjectCollection("spic_entryentity").clear();
            this_dy.getDynamicObjectCollection("spic_entryentity").addAll(this_dy_entrys);
            OperationServiceHelper.executeOperate("save","spic_reports_run_values",new DynamicObject[]{this_dy}, OperateOption.create());
//            SaveServiceHelper.save(new DynamicObject[]{this_dy});
            this_dy.set("spic_status","CY");
            this_dy.set("spic_return_msg","e成功");
            this_dy.set("spic_return_msg_tag","e成功");

            return new PairEn(true,strpkId + "：成功");
//            this.getView().showMessage("导出：" + this_dy_entrys.size() + "条\r\n" + outMsgList.stream().collect(Collectors.joining("\r\n","","\r\n")) + "成功");
        } catch (Exception e) {
            String eMsg = getStackTrace(e);
            this_dy.set("spic_status","CN");
            this_dy.set("spic_return_msg","C失败");
            this_dy.set("spic_return_msg_tag",eMsg);

            return new PairEn(false,strpkId + "：失败" + eMsg);
//            this.getView().showErrMessage(e.getMessage(),getStackTrace(e));
            // excuteInfo.put("success", false);
            // excuteInfo.put("message", ThrowableUtils.getStackTrace(e));
            // logMonitor.error("数据开始导入数据库：错误信息为：" );
        } finally {
            OperationServiceHelper.executeOperate("save","spic_reports_run_values",new DynamicObject[]{this_dy}, OperateOption.create());
        }
    }
    //将报错的方法的所有报错转成string
    public static String getStackTrace(Throwable throwable){
        if (null != throwable) {
            StringWriter stringWriter=new StringWriter();
            PrintWriter printWriter=new PrintWriter(stringWriter);
            try {
                throwable.printStackTrace(printWriter);
                return throwable.getMessage() + stringWriter.toString();
            }finally {
                printWriter.close();
            }
        } else {
            return "";
        }
    }
}
