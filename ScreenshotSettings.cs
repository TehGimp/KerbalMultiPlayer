using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

public class ScreenshotSettings
{

	public const float IDEAL_ASPECT_RATIO = 16.0f / 9.0f;
	//public const float IDEAL_ASPECT_RATIO = 1f;

	private int mMaxWidth;
	private int mMaxHeight;

	public const int MIN_HEIGHT = 128;
	public const int MIN_WIDTH = (int)(MIN_HEIGHT * IDEAL_ASPECT_RATIO);

	public const int MAX_HEIGHT = 512;
	public const int MAX_WIDTH = (int)(MAX_HEIGHT * IDEAL_ASPECT_RATIO);

	public ScreenshotSettings()
	{
        //maxWidth = maxWidth;
        //maxHeight = maxHeight;
        maxHeight = 256; // I don't know why this variable is here
	}

	public int maxWidth
	{
		set
		{
			mMaxWidth = Math.Min(Math.Max(value, MIN_WIDTH), MAX_WIDTH);
			mMaxHeight = (int)Math.Round(mMaxWidth/IDEAL_ASPECT_RATIO);
		}
		get
		{
			return mMaxWidth;
		}
	}

	public int maxHeight
	{
		set
		{
			mMaxHeight = Math.Min(Math.Max(value, MIN_HEIGHT), MAX_HEIGHT);
			mMaxWidth = (int)Math.Round(mMaxHeight*IDEAL_ASPECT_RATIO);
		}
		get
		{
			return mMaxHeight;
		}
	}

	public void getBoundedDimensions(int width, int height, ref int bounded_w, ref int bounded_h)
	{
		float aspect = (float)width / (float)height;

		if (aspect > IDEAL_ASPECT_RATIO)
		{
			//Wider than ideal aspect ratio
			bounded_w = maxWidth;
			bounded_h = Math.Min(maxHeight, (int)Math.Round(maxWidth / aspect));
		}
		else
		{
			//Taller than ideal aspect ratio
			bounded_h = maxHeight;
			bounded_w = Math.Min(maxWidth, (int)Math.Round(maxHeight * aspect));
		}
	}

	public int maxNumBytes
	{
		get
		{
			return maxWidth * maxHeight * 3;
		}
	}

}
