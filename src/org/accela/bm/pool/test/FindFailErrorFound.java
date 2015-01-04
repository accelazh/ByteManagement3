package org.accela.bm.pool.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class FindFailErrorFound
{
	public int find() throws IOException
	{
		return this.find(new LinkedList<String>());
	}

	public int find(List<String> skips) throws IOException
	{
		int count = 0;

		BufferedReader in = new BufferedReader(new FileReader(
				"junit_test_log.txt"));
		int lineNO = 1;
		String line = null;
		while ((line = in.readLine()) != null)
		{
			if (skips.contains(line))
			{
				continue;
			}

			String lineLow = line.toLowerCase();
			if (lineLow.contains("fail")
					|| lineLow.contains("error")
					|| lineLow.contains("found")
					|| lineLow.contains("broken")
					|| lineLow.contains("exception"))
			{
				count++;
				System.out.println(lineNO + ": " + line);
			}

			lineNO++;
		}
		System.out.println("[Complete]");

		return count;
	}

	public static void main(String[] args) throws IOException
	{
		// System.setOut(new PrintStream(new FileOutputStream(
		// "FindFailErrorFound_out.txt")));
		new FindFailErrorFound().find();
	}

}
