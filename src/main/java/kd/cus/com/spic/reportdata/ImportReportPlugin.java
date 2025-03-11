package kd.cus.com.spic.reportdata;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.wsdl.Types;

import kd.bos.dataentity.OperateOption;
import kd.bos.dataentity.entity.DynamicObject;
import kd.bos.db.DB;
import kd.bos.db.DBRoute;
import kd.bos.db.ResultSetHandler;
import kd.bos.db.SqlParameter;
import kd.bos.entity.plugin.AbstractOperationServicePlugIn;
import kd.bos.entity.plugin.args.BeforeOperationArgs;
import kd.bos.logging.Log;
import kd.bos.logging.LogFactory;
import kd.bos.orm.query.QCP;
import kd.bos.orm.query.QFilter;
import kd.bos.servicehelper.BusinessDataServiceHelper;
import kd.bos.servicehelper.operation.OperationServiceHelper;
import kd.fi.bcm.business.olap.OlapSaveBuilder;

import kd.fi.bcm.business.serviceHelper.OlapServiceHelper;
import kd.fi.bcm.business.sql.SQLBuilder;
import kd.fi.bcm.business.sql.MDResultSet;
import kd.fi.bcm.common.Pair;

import java.util.Map;
import java.util.stream.Collectors;
import java.math.BigDecimal;


public class ImportReportPlugin extends AbstractOperationServicePlugIn {

    private static final String UAT = "cwgx";
    private static Log logMonitor = LogFactory.getLog(ImportReportPlugin.class);

    @Override
    public void beforeExecuteOperationTransaction(BeforeOperationArgs e) {
//获取selectids
        try {
            String key = e.getOperationKey();
            List<String> ids = Arrays.stream(e.getDataEntities()).map(DynamicObject::getPkValue).map(i -> i.toString()).collect(Collectors.toList());
            if (ids.size() > 0) {
                String billName = e.getDataEntities()[0].getDynamicObjectType().getName();//获取单据名称
                //选定的id数量
                int getIdsNum = ids.size();
                Map<Object, DynamicObject> this_dys = BusinessDataServiceHelper.loadFromCache(billName, new QFilter[]{new QFilter("id", QCP.in, ids)});
                //获取n条数据
                int getFoundDysNum = this_dys.size();
                int getSucDysNum = 0;
                int getFalDysNum = 0;
                //开始进行操作
                List<PairEn> results = new ArrayList<>();
                results.add(new PairEn(true,""));
                results.add(new PairEn(false,""));
                this_dys.entrySet().forEach(m -> {
                    PairEn result = new PairEn(false, "未处理");
                    if ("spic_importbtn".equals(key)) {
                        result = saveDimension_values(m.getValue());
                    } else if ("spic_exportbtn".equals(key)) {
                        result = exportDimension_values(m.getValue());
                    } else { }
                    results.add(result);
                });
                StringBuffer resultsMsg = new StringBuffer();
                Map<Boolean, List<PairEn>> resultsStream = results.stream().collect(Collectors.groupingBy(success -> success.getSuc()));
                resultsMsg.append("成功：\r\n" + (resultsStream.get(true).size() - 1) + "条");
                resultsMsg.append(resultsStream.get(true).stream().map(i->i.getMsg()).collect(Collectors.joining("\r\n", "", "\r\n")));
                resultsMsg.append("失败：\r\n" + (resultsStream.get(false).size() - 1) + "条");
                resultsMsg.append(resultsStream.get(false).stream().map(i->i.getMsg()).collect(Collectors.joining("\r\n", "", "\r\n")));
                logMonitor.info("ImportReportPlugin\r\n" + resultsMsg);
                e.setCancel(false);
                getOperationResult().setMessage(resultsMsg.toString());
            } else {
                //未选定id
                getOperationResult().setMessage("未选定单据");
            }
        }catch (Exception ex){
            String exMsg = getStackTrace(ex);
            logMonitor.info("ImportReportPlugin\r\n" + exMsg);
            e.setCancel(true);
            e.setCancelMessage(exMsg);
        }
    }

//    @Override
//    public void itemClick(ItemClickEvent evt) {
//        super.itemClick(evt);
//        Object pkid = this.getModel().getValue("id");
//        String key =  evt.getItemKey();
//        System.out.println("0 pkid:" + pkid);
//        BasedataEdit reportID = this.getControl("spic_run_report");// getValue("spic_run_report");
//        DynamicObject report = BusinessDataServiceHelper.loadSingleFromCache("spic_transfer_reports",new QFilter[]{new QFilter("id", QCP.equals,reportID.toString())});
//        String strCub =report.getString("spic_cubenumber");
//        System.out.println("1.5 strCub:" + strCub);
//        if ("spic_importbtn".equals(key))
//            saveDimension_values(pkid);
//        else if("spic_exportbtn".equals(key))
//            exportDimension_values(pkid);
//    }

    private PairEn saveDimension_values(DynamicObject this_dy) {
        /*
         * 维度： "Process", // 过程 "Currency", // 币别 "AuditTrail", // 审计线索 "ChangeType", //
         * 变动类型 "MultiGAAP", // 准则 "Entity", // 组织 "Account", // 科目 "Year", // 年度
         * "Period", // 期间 "Scenario", // 情景 "InternalCompany", // 往来组织 "C1", // 业务版块
         * "C2", // 计量单位 "C3" // YSGD
         */
        String strpkId = this_dy.getPkValue().toString();
        DynamicObject run_report_dy = this_dy.getDynamicObject("spic_run_report");
        // Map<String, Object> excuteInfo = new HashMap<>();
        String modelNum = run_report_dy.getString("spic_cubenumber");
        try {
            logMonitor.info("数据开始导入数据库：" + strpkId + "，modelNum为："+modelNum);

            Object reportOrg= this_dy.get("spic_entity_code");
            logMonitor.info("数据开始导入数据库：" + strpkId + "，reportOrg.toString()："+reportOrg.toString());
            DynamicObject orgs = BusinessDataServiceHelper.loadSingleFromCache("spic_merge_orgs",new QFilter[]{new QFilter("number", QCP.equals,reportOrg.toString())});


            List<SqlParameter> params = new ArrayList<>();
            String strSQL ="";
            if(null !=orgs && "true".equalsIgnoreCase(orgs.getString("spic_is_merge")) ) //合并体
            {
                logMonitor.info("数据开始导入数据库：" + strpkId + "，spic_is_merge："+"true");
                 // SqlParameter[] param = new SqlParameter[]{new SqlParameter(":FId",
                // SqlParameter.type_string, strpkId)};
                System.out.println("1.5 param:" + strpkId);
                 strSQL = " SELECT trr.fbillno, " +
                         " trr.fk_spic_entity_code, " +
                         " trr.fk_spic_currency, " +
                         " trr.fk_spic_period, " +
                         " trr.fk_spic_year, " +
                         " trsv.fk_spic_process_dimensi_c fk_spic_process_dimension, " +
                         " trsv.fk_spic_audit_dimension_c  fk_spic_audit_dimension, " +
                         " trsv.fk_spic_changetyp_dim_c fk_spic_changetyp_dimensi, " +
                         " trsv.fk_spic_multigaap_dim_c fk_spic_multigaap_dimensi, " +
                         " trsv.fk_spic_account_dim_c fk_spic_account_dimension, " +
                         " trsv.fk_spic_scenario_dim_c fk_spic_scenario_dimensio, " +
                         " trsv.fk_spic_internalc_dim_c fk_spic_internalc_dimensi," +
                         " trsv.fk_spic_c1_dim_c fk_spic_c1_dimension, " +
                         " trsv.fk_spic_c2_dim_c fk_spic_c2_dimension, " +
                         " trsv.fk_spic_c3_dim_c fk_spic_c3_dimension, " +
                         " trsv.fk_spic_c4_dim_c fk_spic_c4_dimension, " +
                         " trsv.fk_spic_c5_dim_c fk_spic_c5_dimension, " +
                         " trsv.fk_spic_c6_dim_c fk_spic_c6_dimension, " +
                         " trv.fk_spic_value " +
                         " FROM "+UAT+"_secd.tk_spic_report_run trr, " +
                         " "+UAT+"_secd.tk_spic_report_values trv, " +
                         " "+UAT+"_secd.tk_spic_report_set trs, " +
                         " "+UAT+"_secd.tk_spic_report_merge_v trsv " +
                         " WHERE trr.FId = trv.FId " +
                         " AND trr.fk_spic_run_report = trs.fk_spic_transfer_reports " +
                         " and trs.fk_spic_effictive_year= trr.fk_spic_year" +
                         " AND trs.FId = trsv.FId AND trv.fk_spic_col_num = trsv.fk_spic_report_col_c " +
                         " AND trv.fk_spic_row_num = trsv.fk_spic_report_row_c" + " AND trr.FId = ?";
            }
            else//单体
            {
                // SqlParameter[] param = new SqlParameter[]{new SqlParameter(":FId",
                // SqlParameter.type_string, strpkId)};
                System.out.println("1.6 param:" + strpkId);
                 strSQL = " SELECT trr.fbillno, " +
                         " trr.fk_spic_entity_code, " +
                         " trr.fk_spic_currency, " +
                         " trr.fk_spic_period, " +
                         " trr.fk_spic_year, " +
                         " trsv.fk_spic_process_dimension, " +
                         " trsv.fk_spic_audit_dimension, " +
                         " trsv.fk_spic_changetyp_dimensi, " +
                         " trsv.fk_spic_multigaap_dimensi, " +
                         " trsv.fk_spic_account_dimension, " +
                         " trsv.fk_spic_scenario_dimensio, " +
                         " trsv.fk_spic_internalc_dimensi, " +
                         " trsv.fk_spic_c1_dimension, " +
                         " trsv.fk_spic_c2_dimension, " +
                         " trsv.fk_spic_c3_dimension, " +
                         " trsv.fk_spic_c4_dimension, " +
                         " trsv.fk_spic_c5_dimension, " +
                         " trsv.fk_spic_c6_dimension, " +
                         " trv.fk_spic_value " +
                         " FROM "+UAT+"_secd.tk_spic_report_run trr, " +
                         " "+UAT+"_secd.tk_spic_report_values trv, " +
                         " "+UAT+"_secd.tk_spic_report_set trs, " +
                         " "+UAT+"_secd.tk_spic_report_v trsv " +
                         " WHERE trr.FId = trv.FId " +
                         " AND trr.fk_spic_run_report = trs.fk_spic_transfer_reports " +
                         " AND trs.FId = trsv.FId " +
                         " and trs.fk_spic_effictive_year= trr.fk_spic_year" +
                         " AND trv.fk_spic_col_num = trsv.fk_spic_report_col " +
                         " AND trv.fk_spic_row_num = trsv.fk_spic_report_row" + "   AND trr.FId = ?";
            }
            System.out.println("2 strpkId:" + strpkId);
            System.out.println("0 modelNum:" + modelNum);
            OlapSaveBuilder save = new OlapSaveBuilder(modelNum);
            save.setCrossDimensions("Process", "Currency", "AuditTrail", "ChangeType", "MultiGAAP", "Entity", "Account",
                    "Year", "Period", "Scenario", "InternalCompany", "C1", "C2", "C3", "C4", "C5", "C6");
            save.setMeasures("FMONEY");
            System.out.println("1 strpkId:" + strpkId);
            
            params.add(new SqlParameter(":FId", Types.STRING_TYPE, strpkId.toString()));
            System.out.println("2.1 add params strpkId:" + strpkId);
            logMonitor.info("7 strpkId: ：" + strpkId );
            List<Pair<String[], Object>> saveValPairs = new ArrayList<>();
            ResultSetHandler<List<String>> action = (rs) -> {
                System.out.println("3 strpkId:" + strpkId);
                // List<report_dimension> dimensions = new ArrayList<>(10);

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
                    BigDecimal money = rs.getBigDecimal("fk_spic_value");
                //    System.out.println("process:" + process +";strpkId:"+ strpkId);
                   // System.out.println("money:" + money +";strpkId:"+ strpkId+";account:"+account);
                    logMonitor.info("71 process: ：" + process+";strpkId:"+ strpkId );
                    logMonitor.info("72 money: ：" + money+";strpkId:"+ strpkId+";account:"+account );
                    saveValPairs
                            .add(Pair
                                    .onePair(
                                            new String[] { process, currency, auditTrail, changeType, multiGAAP, entity,
                                                    account, year, period, scenario, internalCompany, c1, c2, c3, c4, c5, c6  },
                                            money));
                    save.setCellSet(saveValPairs);
                    logMonitor.info("save:"  +";strpkId:"+ strpkId);
                }
              /*  StringBuffer loginfo;
                saveValPairs.stream().forEach(m->{
                    (PairEn)m.p1
                });
                */
                logMonitor.info("before  doSave:"  +";strpkId:"+ strpkId);
                save.doSave();
                logMonitor.info("doSave:"  +";strpkId:"+ strpkId);
                return null;
            };
            System.out.println("5 strSQL:" + strSQL);
            List<String> rowId = DB.query(DBRoute.meta, strSQL, params.toArray(new SqlParameter[params.size()]),
                    action);
            System.out.println("6 strpkId:" + strpkId);

            this_dy.set("spic_status","CY");
            this_dy.set("spic_return_msg","C成功");
            this_dy.set("spic_return_msg_tag","C成功");
            return new PairEn(true,strpkId + "：成功");
//            this.getView().showMessage("成功");
        } catch (Exception e) {
            String eMsg = getStackTrace(e);
            this_dy.set("spic_status","CN");
            this_dy.set("spic_return_msg","C失败");
            this_dy.set("spic_return_msg_tag",eMsg);

            return new PairEn(false,strpkId + "：失败" + eMsg);
//            this.getView().showErrMessage(e.getMessage(),getStackTrace(e));
//            System.out.println("7 strpkId: exception" + getStackTrace(e));
//            logMonitor.error("7 strpkId: exception：" + strpkId + "："+ getStackTrace(e));
            // excuteInfo.put("success", false);
            // excuteInfo.put("message", ThrowableUtils.getStackTrace(e));
            // logMonitor.error("数据开始导入数据库：错误信息为：" );
        }finally {
            OperationServiceHelper.executeOperate("save","spic_reports_run_values",new DynamicObject[]{this_dy}, OperateOption.create());
        }

    }

    private PairEn exportDimension_values(DynamicObject this_dy) {
        String strpkId = this_dy.getPkValue().toString();
        Log logMonitor = LogFactory.getLog(ImportReportPlugin.class);
        List<DynamicObject> this_dy_entrys = new ArrayList<>();
        List<String> outMsgList = new ArrayList<>();
        // Map<String, Object> excuteInfo = new HashMap<>();
        DynamicObject run_report_dy = this_dy.getDynamicObject("spic_run_report");
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
                         " and trs.fk_spic_effictive_year= trr.fk_spic_year" +
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
                        " and trs.fk_spic_effictive_year= trr.fk_spic_year" +
                        " AND trs.FId = trsv.FId " +
                        " AND trr.FId = ? ";
            }
            System.out.println("2 strpkId:" + strpkId);
            logMonitor.info("开始查询：" + "，strSQL："+strSQL+ ";trr.FId = "+ strpkId);
            params.add(new SqlParameter(":FId", Types.STRING_TYPE, strpkId.toString()));
            System.out.println("2.1 add params strpkId:" + strpkId);
            List<PairEn> saveValPairs = new ArrayList<>();
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
