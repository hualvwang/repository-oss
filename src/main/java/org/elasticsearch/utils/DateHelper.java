package org.elasticsearch.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author hanqing.zhq@alibaba-inc.com
 * @date 2018/4/16
 */
public class DateHelper {
    private static final Logger logger = LogManager.getLogger(DateHelper.class);

    public static Date convertStringToDate(String dateString) {
        String dateRegex = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        SimpleDateFormat sdf = new SimpleDateFormat(dateRegex);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date returnDate = null;
        try {
            returnDate = sdf.parse(dateString);
        } catch (ParseException e) {
            logger.error("convert String to Date type error", e);
        }
        return returnDate;
    }
}
