package net.redheademile.frs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

public class FRSClient
{
	private static boolean enable, connected;
	
	private Runnable runnable;
	private PrintWriter out;
	
	private String ip, password;
	private int port, timeout;
	
	private static Socket s;
	private static InputStream inS;
	private static OutputStream outS;

	private Thread t1, t2;
	private List<String> waiting;
	
	private Map<String, String> responses;
	
	public FRSClient()
	{
		waiting = new ArrayList<>();
	}
	
	/**
	 * Connect to a server
	 * @param out Output to print debug and message
	 * @param port Port of the server
	 * @param password Password of the server (if needed)
	 * @throws IOException 
	 */
	public boolean startConnection(OutputStream out, String ip, int port, String password, int timeout, boolean autoReconnect)
	{
		if(enable) return false;
		if(responses == null) responses = new HashMap<>();

		if(out != null)
		{
			this.out = new PrintWriter(out, true);
			
			this.ip = ip;
			this.port = port;
			this.timeout = timeout;
			this.password = password;
			
			t1 = new Thread()
			{
				@Override
				public void run()
				{
					while(true)
					{
						if(enable)
						{
							try
							{
								if(connected)
								{
									connected = false;
									send("areyouhere", false);
								}
								else
								{
									FRSClient.this.out.println("Time out");
									if (autoReconnect) restart();
									else
									{
										shutdown(true);
										break;
									}
								}
							}
							catch(Exception e) { }
						}
						try
						{
							Thread.sleep(10_000L);
						}
						catch(Exception e) { if(!enable) break; }
					}
				}
			};
			t1.start();
			
			runnable = new Runnable()
			{
				@Override
				public void run()
				{
					enable = true;
					try
					{
						connected = true;
						s = new Socket();
						s.setKeepAlive(true);
						s.connect(new InetSocketAddress(ip, port), 15 * 1000);
						FRSClient.this.out.println("Connected !");
						
						inS = s.getInputStream();
						outS = s.getOutputStream();
						StringBuilder builder = new StringBuilder();
						
						if(waiting != null)
						{
							for(int i = 0; i < waiting.size(); i++) outS.write(waiting.get(i).getBytes());
							waiting.clear();
						}
						
						while(enable)
						{
							try
							{
								int available = inS.available();
								if(available > 0)
								{
									byte[] received = new byte[available];
									inS.read(received);
									builder.append(new String(received));
									
									String result = builder.toString();
									if(result.endsWith("\n"))
									{
										result = new String(result.substring(0, result.length() - 1));
										builder.delete(0, builder.length());
										
										for(String line : result.split("\n"))
										{
											try
											{
												onMessageReceived(line);
											}
											catch(Exception e) { e.printStackTrace(); }
										}
									}
								}
							}
							catch (IOException e) { e.printStackTrace(); }
							try
							{
								Thread.sleep(10);
							}
							catch (InterruptedException e) { }
						}
					}
					catch(IOException e)
					{
						FRSClient.this.out.println("Connection impossible/lost !");
						if (!autoReconnect) shutdown(true);
					}
				}
			};
		}
		
		t2 = new Thread(runnable);
		t2.start();
		
		return true;
	}
	
	private void restart()
	{
		shutdown(false);
		out.println("Retrying...");
		startConnection(null, ip, port, password, timeout, true);
	}
	
	public void shutdown(boolean realShutdown)
	{
		try
		{
			enable = false;
			if(realShutdown && t1 != null) t1.interrupt();
			t2.interrupt();
			s.close();
			if (outS != null) outS.close();
			if (inS != null) inS.close();
		}
		catch(Exception e) { e.printStackTrace(); }
	}
	
	private synchronized String send(String message, boolean needRep) throws IOException
	{
		if(needRep)
		{
			if(s != null && !s.isClosed())
			{
				long time = System.currentTimeMillis();
				String identifier = Long.toString(System.currentTimeMillis());
				outS.write((identifier + " " + message.replace("\n", "\\n") + "\n").getBytes());
				outS.flush();
				while(!responses.containsKey(identifier) && System.currentTimeMillis() - time <= 10000L)
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) { e.printStackTrace(); }
				
				String response = responses.get(identifier);
				if(response != null)
				{
					responses.remove(identifier);
					return response.replace("\\n", "\n");
				}
			}
			return null;
		}
		else
		{
			if(s != null && !s.isClosed() && outS != null && s.isConnected())
			{
				outS.write(("0 " + message.replace("\n", "\\n") + "\n").getBytes());
				outS.flush();
				return null;
			}
			else waiting.add(message);
			return null;
		}
	}
	
	private void onMessageReceived(String message)
	{
		if(message.equals("imhere")) connected = true;
		else
		{
			String[] data = message.split(" ");
			if(data[0].equals("publish"))
			{
				String channel = data[1], msg = "";
				for(int i = 2; i < data.length; i++) msg += (i == 2 ? "" : " ") + data[i];
				
				onPublish(channel, msg);
			}
			else if(data[0].equals("rep"))
			{
				String identifer = data[1];
				String value = "";
				for(int i = 2; i < data.length; i++) value += (i == 2 ? "" : " ") + data[i];
				
				responses.put(identifer, value);
			}
		}
	}
	
	private void onPublish(String channel, String message)
	{
		out.println("Message received from channel \"" + channel + "\": " + message);
	}
	
	public boolean isConnected()
	{
		return connected || s != null;
	}
	
	public void publish(String channel, String message)
	{
		try
		{
			send("publish " + channel + " " + message, false);
		}
		catch (IOException e) { e.printStackTrace(); }
	}
	
	public String getValue(String key, String field)
	{
		try
		{
			String result = send("get " + (key != null ? key.replace(' ', '_') : "null") + " " + (field != null ? field.replace(' ', '_') : "null"), true);
			if(result == null || result.isEmpty()) return null;
			return result;
		}
		catch(Exception e) { return null; }
	}
	
	public Set<String> getFields(String key)
	{
		try
		{
			String result = send("getall " + key.replace(' ', '_'), true);
			if(result == null || result.isEmpty()) return Collections.emptySet();
			Set<String> keys = new HashSet<>();
			Collections.addAll(keys, result.split(" "));
			return keys;
		}
		catch(Exception e) { return Collections.emptySet(); }
	}
	
	public Map<String, String> getValues(String key, Set<String> fields)
	{
		try
		{
			HashMap<String, String> valuesPerFields = new HashMap<String, String>();
			for(String field : fields)
			{
				String value = getValue(key, field);
				valuesPerFields.put(field, value);
			}
			return valuesPerFields;
		}
		catch(Exception e) { return Collections.emptyMap(); }
	}
	
	public void setValue(String key, String field, String value)
	{
		try
		{
			send("set " + (key != null ? key.replace(' ', '_') : "null") + " " + (field != null ? field.replace(' ', '_') : "null") + " " + value, false);
		}
		catch (IOException e) { e.printStackTrace(); }
	}
	
	public static void main(String[] args)
	{
		FRSClient client = new FRSClient();

		Scanner in = new Scanner(System.in);
		while (true)
		{
			String line = in.nextLine();
			if (line != null && !line.isEmpty())
			{
				String[] data = line.split(" ");
				if (line.startsWith("start"))
				{
					try
					{
						if (client.startConnection(System.out, data[1], Integer.valueOf(data[2]), null, 10000, false))
							System.out.println("Trying to connect..");
						else
							System.out.println("Already connected !");
					}
					catch (Exception e) { System.out.println("start [<ip>] [<port>]"); }
				}
				else if (line.equalsIgnoreCase("shutdown") || line.equalsIgnoreCase("stop"))
				{
					System.out.println("Shutting down..");
					client.shutdown(true);
					in.close();
					break;
				}
				else if (line.startsWith("set"))
				{
					try
					{
						if (data.length >= 4)
						{
							client.setValue(data[1], data[2], line.substring(4 + data[1].length() + 1 + data[2].length() + 1));
							System.out.println("Value set !");
						}
						else System.out.println("set [<key>] [<field>] [<value>]");
					}
					catch (Exception e) { System.out.println("set [<key>] [<field>] [<value>]"); }
				}
				else if (line.startsWith("getall"))
				{
					try
					{
						System.out.println(client.getFields(data[1]).toString());
					}
					catch (Exception e) { System.out.println("getall [<key>]"); }
				}
				else if (line.startsWith("get"))
				{
					try
					{
						System.out.println(client.getValue(data[1], data[2]));
					}
					catch (Exception e) { System.out.println("get [<key>] [<field>]"); }
				}
				else if (line.startsWith("publish"))
				{
					if (data.length >= 3)
					{
						client.publish(data[1], line.substring(8 + data[1].length() + 1));
						System.out.println("Published !");
					}
					else System.out.println("publish [<channel>] [<message>]");
				}
				else System.out.println("start | stop | set | getall | get");
			}
			else System.out.println("start | stop | set | getall | get");
		}
	}
}