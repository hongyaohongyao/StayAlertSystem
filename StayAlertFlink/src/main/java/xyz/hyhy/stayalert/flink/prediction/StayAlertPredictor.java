package xyz.hyhy.stayalert.flink.prediction;

import org.dmg.pmml.FieldName;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.EvaluatorUtil;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.InputField;
import org.xml.sax.SAXException;
import xyz.hyhy.stayalert.flink.utils.PMMLUtils;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StayAlertPredictor {
    private Evaluator evaluator;
    private List<InputField> inputFields;

    public StayAlertPredictor() throws IOException, JAXBException, SAXException {
        evaluator = PMMLUtils.loadEvaluator("/LightStayAlertRFC.pmml");
        inputFields = evaluator.getInputFields();
    }

    public Boolean predict(Map<String, ?> inputRecord) {
        if (inputRecord == null) {
            throw new NullPointerException("预测程序不能输入空的记录");
        }
        Map<FieldName, FieldValue> arguments = new LinkedHashMap<>();
        // 从数据源模式到PMML模式逐字段映射记录
        for (InputField inputField : inputFields) {
            FieldName inputName = inputField.getName();

            Object rawValue = inputRecord.get(inputName.getValue());
            Double doubleValue = Double.parseDouble(rawValue.toString());
            // 将任意用户提供的值转换为已知的PMML值
            FieldValue inputValue = inputField.prepare(doubleValue);
            arguments.put(inputName, inputValue);
        }
        // 用已知的特征来评估模型
        Map<FieldName, ?> results = evaluator.evaluate(arguments);

        // 解耦结果来自jpmml-evaluator运行时环境
        Map<String, ?> resultRecord = EvaluatorUtil.decodeAll(results);
        //获取并返回预测结果
        Integer isAlert = (Integer) resultRecord.get("IsAlert");

        return isAlert == 1;
    }

}
