package com.thor.node.core.process;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.Charset;

/**
 * Thor 字符集转码引擎 (替代老 FEX 的系统 iconv 命令)
 * 纯 Java NIO 流式处理，防 OOM，严格遵循 0x0A 换行规范
 */
@Component
public class IconvProcessor {
    private static final Logger log = LoggerFactory.getLogger(IconvProcessor.class);

    /**
     * 流式转码方法
     * @param sourcePath 源文件路径
     * @param destPath   目标文件路径
     * @param fromCharset 源编码 (如 GBK)
     * @param toCharset   目标编码 (如 UTF-8)
     * @return 是否转码成功
     */
    public boolean convertEncoding(String sourcePath, String destPath, String fromCharset, String toCharset) {
        log.info(">>> [数据清洗] 启动 Iconv 转码引擎: [{}] -> [{}], 目标文件: {}", fromCharset, toCharset, sourcePath);
        File srcFile = new File(sourcePath);
        File destFile = new File(destPath);

        if (!srcFile.exists()) {
            log.error(">>> [拦截] 转码失败，源文件不存在: {}", sourcePath);
            return false;
        }

        // 使用带缓冲的字符流进行极速读取与写入
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(srcFile), Charset.forName(fromCharset)));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(destFile), Charset.forName(toCharset)))) {

            String line;
            long lineCount = 0;
            boolean isFirstLine = true; // 【新增】首行标志位

            while ((line = reader.readLine()) != null) {
                // 【核心修复】：将 \n 加在上一行的末尾，而不是当前行的末尾，避免最后多出一个空行
                if (!isFirstLine) {
                    writer.write("\n");
                }
                writer.write(line);
                isFirstLine = false;
                lineCount++;
            }
            log.info(">>> [数据清洗] 转码大捷！共处理 {} 行，输出文件: {}", lineCount, destPath);
            return true;

        } catch (Exception e) {
            log.error(">>> [异常] Iconv 转码过程发生崩溃", e);
            return false;
        }
    }
}