package com.hbaseclient;

import org.apache.commons.lang.StringUtils;

public class UrlUtils {
    public static String url2Mid(String docurl) {

        String docid = null;
        if (StringUtils.trimToNull(docurl) != null) {
            int idx = docurl.indexOf("?");
            if (idx > 0) {
                docurl = docurl.substring(0, idx);
            }

            idx = docurl.lastIndexOf("/");
            if (idx > 0) {
                docurl = docurl.substring(idx + 1);
            }

            if (StringUtils.trimToNull(docurl) != null && docurl.length() == 9) {
                StringBuilder sb = new StringBuilder();
                sb.append(Long.toString(Base62.decode(docurl.substring(0, 1))));
                System.out.printf(Base62.decode(docurl.substring(0, 1))+"#"+sb.toString());
                sb.append(String.format("%07d", Base62.decode(docurl.substring(1, 5))));
                System.out.printf(sb.toString());
                sb.append(String.format("%07d", Base62.decode(docurl.substring(5, 9))));
                System.out.printf(sb.toString());
                docid = sb.toString();
            }
        }
        return docid;
    }

}
