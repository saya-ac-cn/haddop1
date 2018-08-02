package ac.cn.saya.youTube.util;

import org.apache.log4j.Logger;

/**
 * 数据清洗单元
 */
public class ETLUtil {

    private static final Logger LOGGER = Logger.getLogger(ETLUtil.class);

    public static String getETLString(String ori)
    {
        StringBuilder sb = new StringBuilder();
        String [] splitArray = ori.split("\t");
        if(splitArray.length < 9)
            return null;
        for(int i = 0;i < splitArray.length;i++)
        {
            sb.append(splitArray[i]);
            if(i < 9)
            {
                if(i != splitArray.length - 1)
                {
                    sb.append("\t");
                }
            }
            else
            {
                if(i != splitArray.length - 1)
                {
                    sb.append("&");
                }
            }
        }
        return sb.toString();
    }

    public static void main(String [] args)
    {
        String text ="LKh7zAJ4nwo	TheReceptionist	653	Entertainment	424	13021	4.34	1305	744	DjdA-5oKYFQ	NxTDlnOuybo	c-8VuICzXtU	DH56yrIO5nI	W1Uo5DQTtzc	E-3zXq_r4w0	1TCeoRPg5dE	yAr26YhuYNY	2ZgXx72XmoE	-7ClGo-YgZ0	vmdPOOd6cxI	KRHfMQqSHpk	pIMpORZthYw	1tUDzOp10pk	heqocRij5P0	_XIuvoH6rUg	LGVU5DsezE0	uO2kj6_D8B4	xiDqywcDQRM	uX81lMev6_o";
        System.out.println(getETLString(text));
    }
}