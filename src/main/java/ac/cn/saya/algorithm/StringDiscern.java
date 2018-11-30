package ac.cn.saya.algorithm;

/**
 * @Title: StringDiscern
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/11/30 22:21
 * @Description:
 * 文本提取特征值
 */

public class StringDiscern {

    /**
     * 	根据处方数据和处方模板读取用法相关的数据
     * 	提取:口服；1次1mg；1日3次；
     * 	模板:口服；1次#{dosage}mg；#{day}日#{frequency}次；
     * @Title: studyDosage
     * @Description: TODO(这里用一句话描述这个方法的作用)
     * @param
     * @return String[]
     * @throws
     */
    protected static String[] studyDosage(String dosageStr,String templateStr)
    {
        // 存放返回的数据，0：规格；1：天；2：频率
        String[] result = null;
        int dosageIndex = templateStr.indexOf("#{dosage}");// 在模板中得到#{dosage}的位置，在实际的用法用量中得到，每次的剂量
        if(dosageIndex == -1)
        {
            System.err.println(dosageStr+",用法学习失败。");
            return result;
        }

        /**
         * 	开始读取 用法中 剂量的数据
         */
        StringBuilder dosageBf = new StringBuilder();// 获取到的 剂量
        for(int i=dosageIndex;i < dosageStr.length();i++)
        {
            char c = dosageStr.charAt(i);
            if (Character.isDigit(c) || c == '.') {
                // 是数字
                dosageBf.append(c);
                continue;
            }else
            {
                // 不是数字
                break;
            }
        }
        if(dosageBf.length() == 0)
        {
            System.err.println(dosageStr+",用法学习失败。");
            return result;
        }

        /**
         * 得到天
         */
        //把上一步得到的剂量，还原到模板字符串中。
        String reductionStr = templateStr;
        reductionStr = reductionStr.replace("#{dosage}",dosageBf.toString());
        int dayIndex = reductionStr.indexOf("#{day}");// 在模板中得到#{dosage}的位置，在实际的用法用量中得到，每次的剂量
        if(dayIndex == -1)
        {
            System.err.println(dosageStr+",用法学习失败。");
            return result;
        }
        StringBuilder dayBf = new StringBuilder();// 获取到的 天 数字
        for(int i=dayIndex;i < dosageStr.length();i++)
        {
            char c = dosageStr.charAt(i);
            if (Character.isDigit(c)) {
                // 是数字
                dayBf.append(c);
                continue;
            }else
            {
                // 不是数字
                break;
            }
        }
        if(dayBf.length() == 0)
        {
            System.err.println(dosageStr+",用法学习失败。");
            return result;
        }

        /**
         * 得到频率
         */
        reductionStr = reductionStr.replace("#{day}",dayBf.toString());
        int frequencyIndex = reductionStr.indexOf("#{frequency}");// 在模板中得到#{frequency}的位置，在实际的用法用量中得到频率
        if(frequencyIndex == -1)
        {
            System.err.println(dosageStr+",用法学习失败。");
            return result;
        }
        StringBuilder frequencyBf = new StringBuilder();// 获取到的 频率 数字
        for(int i=frequencyIndex;i < dosageStr.length();i++)
        {
            char c = dosageStr.charAt(i);
            if (Character.isDigit(c)) {
                // 是数字
                frequencyBf.append(c);
                continue;
            }else
            {
                // 不是数字
                break;
            }
        }
        if(frequencyBf.length() == 0)
        {
            System.err.println(dosageStr+",用法学习失败。");
            return result;
        }
        result = new String[3];
        result[0] = dayBf.toString();
        result[1] = frequencyBf.toString();
        result[2] = dosageBf.toString();
        return result;
    }

    public static void main(String[] args)
    {
        studyDosage("口服；1次1.5mg；2日5次；","口服；1次#{dosage}mg；#{day}日#{frequency}次；");
    }

}
