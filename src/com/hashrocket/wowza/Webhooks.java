package com.hashrocket.wowza;

import com.wowza.wms.application.*;
import com.wowza.wms.amf.*;
import com.wowza.wms.client.*;
import com.wowza.wms.module.*;
import com.wowza.wms.request.*;
import com.wowza.wms.stream.*;

import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.net.HttpURLConnection;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class Webhooks extends ModuleBase {

	public void publish(IClient client, RequestFunction function, AMFDataList params) {
		String streamName = getParamString(params,PARAM1);
		IMediaStream stream = getStream(client, function);
		getLogger().info("publish: clientID = " + client.getClientId());
		getLogger().info("publish: streamSrc = " + stream.getSrc());
		getLogger().info("publish: streamName = " + streamName);

		stream.getProperties().setProperty("startedAt", System.currentTimeMillis());

		WMSProperties localWMSProperties = client.getAppInstance().getProperties();
		String onConnectURL = localWMSProperties.getPropertyStr("webhooksPublishURL");

		try {
			String token = parseQuery(client.getQueryStr()).get("token");
			JsonObject result = new JsonObject();
			result.add("id", new JsonPrimitive(client.getClientId()));
			result.add("ip", new JsonPrimitive(client.getIp()));
			result.add("stream_name", new JsonPrimitive(streamName));
			String json = result.toString();
			String resp = post(onConnectURL + "?auth_token=" + token, json);
			getLogger().info("publish to " + streamName + " authenticated: " + resp);
		} catch(Throwable e) {
			getLogger().info("publish to " + streamName + " unauthenticated: " + client.getClientId());
			shutdown(client, function, params);
		} finally {
			invokePrevious(client, function, params);
		}
	}

	public void onStreamDestroy(IMediaStream stream) throws UnsupportedEncodingException {
		IClient client = stream.getClient();
		String streamName = stream.getName();
		getLogger().info("onStreamDestroy: clientID = " + client.getClientId());
		getLogger().info("onStreamDestroy: streamSrc = " + stream.getSrc());
		getLogger().info("onStreamDestroy: streamName = " + streamName);

		long startedAt = stream.getProperties().getPropertyLong("startedAt", -1);
		long durationMs = System.currentTimeMillis() - startedAt;

		WMSProperties localWMSProperties = client.getAppInstance().getProperties();
		String onConnectURL = localWMSProperties.getPropertyStr("webhooksUnpublishURL");
		String pathPrefix = localWMSProperties.getPropertyStr("webhooksRecordingPathPrefix");

		try {
			String token = parseQuery(client.getQueryStr()).get("token");
			JsonObject result = new JsonObject();
			result.add("id", new JsonPrimitive(client.getClientId()));
			result.add("ip", new JsonPrimitive(client.getIp()));
			result.add("stream_name", new JsonPrimitive(streamName));
			result.add("duration", new JsonPrimitive(durationMs));
			result.add("url", new JsonPrimitive(pathPrefix + streamName + ".mp4"));
			String json = result.toString();
			String resp = post(onConnectURL + "?auth_token=" + token, json);
			getLogger().info("unpublish to " + streamName + " authenticated: " + resp);
		} catch(Throwable e) {
			getLogger().info("unpublish to " + streamName + " unauthenticated: " + client.getClientId());
		}
	}

	private static void shutdown(IClient client, RequestFunction function, AMFDataList params) {
		IMediaStream stream = getStream(client, function);
		String streamName = getParamString(params, PARAM1);
		client.shutdownClient();
		client.setShutdownClient(true);
		stream.shutdown();
		sendStreamOnStatusError(stream, "Unauthorized", "Can't publish to stream: "+ streamName);
	}

	private static String post(String urlString, String json) throws IOException, Unauthenticated {
		URL url;
		HttpURLConnection connection;
		try {
			url = new URL(urlString);
			connection = (HttpURLConnection)url.openConnection();
			try {
				connection.setDoOutput(true);
				connection.setDoInput(true);
				connection.setRequestMethod("POST");
				connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
				connection.setRequestProperty("Accept", "application/json; charset=UTF-8");

				OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());
				out.write(json);
				out.flush();

				BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
				String payload = readAll(in);
				out.close();
				in.close();

				if (connection.getResponseCode() == HttpURLConnection.HTTP_OK ||
						connection.getResponseCode() == HttpURLConnection.HTTP_CREATED) {
					return payload;
				} else {
					throw new Unauthenticated("" + connection.getResponseCode());
				}
			}catch (Throwable e) {
				throw new Unauthenticated("" + connection.getResponseCode());
			}
		}catch (Throwable e) {
			throw new Unauthenticated(e);
		}
	}

	private static String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}

	private static Map<String, String> parseQuery(String query) throws UnsupportedEncodingException {
		Map<String, String> params = new LinkedHashMap<String, String>();
		String[] pairs = query.split("&");
		for (String pair : pairs) {
			int idx = pair.indexOf("=");
			params.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
		}
		return params;
	}

	private static class Unauthenticated extends Exception {
		private static final long serialVersionUID = 1L;

		private Unauthenticated() {
			super();
		}

		private Unauthenticated(String message) {
			super(message);
		}

		private Unauthenticated(String message, Throwable cause) {
			super(message, cause);
		}

		private Unauthenticated(Throwable cause) {
			super(cause);
		}
	}

}
