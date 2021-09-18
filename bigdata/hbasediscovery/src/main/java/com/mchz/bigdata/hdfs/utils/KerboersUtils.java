package com.mchz.bigdata.hdfs.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.Config;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class KerboersUtils {
	private static ConcurrentHashMap<String, UserGroupInformation> UGI_MAP = new ConcurrentHashMap<String, UserGroupInformation>();
	public static final String KEY_DM_KERBEROS_PRINCIPAL = "dm.kerberos.principal";
	public static final String KEY_DM_KERBEROS_KRB5_CONF = "dm.kerberos.krb5.conf";
	public static final String KEY_DM_KERBEROS_KEYTAB = "dm.kerberos.keytab";
	public static final String DM_HIVE_KERBEROS_ENABLE = "dm.hive.kerberos.enable";
	public static final String[] CONFIG_ARRAY = { KEY_DM_KERBEROS_PRINCIPAL, KEY_DM_KERBEROS_KRB5_CONF,
			KEY_DM_KERBEROS_KEYTAB, DM_HIVE_KERBEROS_ENABLE };
	public static AtomicReference<String> lastLogin = new AtomicReference<String>();
	private static final Logger LOGGER = LoggerFactory.getLogger(KerboersUtils.class);
	private static final boolean IS_WINDOWS = System.getProperties().getProperty("os.name").toUpperCase().indexOf("WINDOWS") != -1;

	public static synchronized UserGroupInformation getLoginUgi(String filePath, String principal, String keytab,
			String confStr, Configuration conf) throws Exception {
		String authority = "";
		try {
			URI uri = new URI(filePath);
			authority = uri.getAuthority();
		} catch (Exception e) {
		}
		if (authority == null || ("").equals(authority)) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("验证地址改为 hive的 地址 " + authority);
			}
			authority = filePath;
		}
		// 判断上次登录的
		String lastUser = lastLogin.get();
		UserGroupInformation ugi = UGI_MAP.get(authority);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("上次登录的user " + lastUser);
		}
		try {
			if (lastUser == null) {
				ugi = login(filePath, principal, keytab, confStr, conf);
				UGI_MAP.put(authority, ugi);
			} else {
				// 不为空
				if (!lastUser.equals(authority) || ugi == null) {
					ugi = login(filePath, principal, keytab, confStr, conf);
					UGI_MAP.put(authority, ugi);
				}
			}
		} catch (Exception e) {
			LOGGER.error("登录KERBOERS错误", e);
		} finally {
			lastLogin.set(authority);
		}
		return ugi;
	}

	public static synchronized UserGroupInformation reLogin(String keyPath, String confStr) throws Exception {
		UserGroupInformation ugi = UGI_MAP.get(keyPath);
		System.setProperty("java.security.krb5.conf", confStr);
		ugi.reloginFromKeytab();
		return ugi;
	}

	public static synchronized UserGroupInformation login(String filePath, String principal, String keytab,
			String confStr, Configuration conf) throws Exception {
        UserGroupInformation.reset();
		if (StringUtils.isEmpty(keytab)) {
			Config.refresh();
			return UserGroupInformation.createRemoteUser(getUserInfo(filePath, principal));
		}
		if (confStr.indexOf("\\\\") > -1) {
			confStr = confStr.replace("\\\\", "\\");
		}
		// LOGGER.info("confStr {}",confStr);
		System.setProperty("java.security.krb5.conf", confStr);
        KerberosName.resetDefaultRealm();
		Config.refresh();
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab(principal, keytab);
		UserGroupInformation loginUserFromKeytabAndReturnUGI = UserGroupInformation
				.loginUserFromKeytabAndReturnUGI(principal, keytab);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("访问 KERBREOS path=>" + filePath);
		}
		return loginUserFromKeytabAndReturnUGI;
	}

	public static String getUserInfo(String filePath, String principal) throws URISyntaxException {
		URI uri = new URI(filePath);
		String userInfo = uri.getUserInfo();
		if (userInfo != null && userInfo.indexOf(":") > -1) {
			String[] split = userInfo.split(":");
			userInfo = split[0];
		} else {
			userInfo = principal;
		}
		if (userInfo == null || userInfo.equals("")) {
			userInfo = "hive";
			LOGGER.error("登录用户获取不到默认设置 为 hive");
		}
		return userInfo;
	}

	public static boolean isKerberosEnbale(Configuration configuration) {
		return configuration.get("hadoop.security.authentication").equalsIgnoreCase("kerberos") ? true : false;
	}

	public static String getKerberosPrincipal(Configuration conf) throws Exception {
		return getKerberosConfigFormHadoopConfiguration(conf, KEY_DM_KERBEROS_PRINCIPAL);
	}

	public static String getKerberosKeytab(Configuration conf) throws Exception {
		return getKerberosConfigFormHadoopConfiguration(conf, KEY_DM_KERBEROS_KEYTAB);
	}

	public static String getKerberoskrb5(Configuration conf) throws Exception {
		return getKerberosConfigFormHadoopConfiguration(conf, KEY_DM_KERBEROS_KRB5_CONF);
	}

	public static synchronized void setKerberosSubjectInfo(Configuration conf, Map<String, String> kerberosSubjectMap)
			throws Exception {
		if (kerberosSubjectMap != null && kerberosSubjectMap.size() > 0) {
			for (String str : CONFIG_ARRAY) {
				String value = kerberosSubjectMap.get(str);
				if (value == null) {
					if (str.equalsIgnoreCase(DM_HIVE_KERBEROS_ENABLE)) {
						conf.set(DM_HIVE_KERBEROS_ENABLE, "true");
						continue;
					}
					throw new Exception("core-site.xml中开启了kerberos,请输入kerberos 相关配置");
				}
				conf.set(str, value);
			}
			// 设置非kerberos 的登录
			conf.setBoolean("ipc.client.fallback-to-simple-auth-allowed", true);
		}
	}

	public static void setKerberosKeytab(Configuration conf, String value) throws Exception {
		conf.set(KEY_DM_KERBEROS_KEYTAB, value);
	}

	public static void setKerberoskrb5(Configuration conf, String value) throws Exception {
		conf.set(KEY_DM_KERBEROS_KRB5_CONF, value);
	}

	public static String getKerberosConfigFormHadoopConfiguration(Configuration conf, String key) throws Exception {
		String confStr = conf.get(key);
		if (StringUtils.isEmpty(confStr)) {
			throw new Exception("配置文件缺少配置 " + key);
		} else {
			return confStr;
		}
	}

}
