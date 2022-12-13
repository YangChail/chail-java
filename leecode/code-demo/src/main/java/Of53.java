import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author : yangc
 * @date :2022/11/18 15:09
 * @description :
 * @modyified By:
 */
public class Of53 {


    public static int search(int[] nums, int target) {
        int left, right, len = nums.length;
        if (nums.length == 0) {
            return 0;
        }
        int min = 0, max = len - 1;
        while (min < max) {
            int tmp = (min + max) / 2;
            if (nums[tmp] >= target) {
                max = tmp;
            } else {
                min = tmp + 1;
            }
        }
        if (nums[max] != target) {
            return 0;
        }
        left = max;
        min = 0;
        max = len - 1;
        while (min < max) {
            int tmp = (min + max + 1) / 2;
            if (nums[tmp] <= target) {
                min = tmp;
            } else {
                max = tmp - 1;
            }
        }
        right = max;
        return right - left + 1;
    }


    public static int[] stringToIntegerArray(String input) {
        input = input.trim();
        input = input.substring(1, input.length() - 1);
        if (input.length() == 0) {
            return new int[0];
        }

        String[] parts = input.split(",");
        int[] output = new int[parts.length];
        for (int index = 0; index < parts.length; index++) {
            String part = parts[index].trim();
            output[index] = Integer.parseInt(part);
        }
        return output;
    }

    public static void main(String[] args) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String line;
        while ((line = in.readLine()) != null) {
            int[] nums = stringToIntegerArray(line);
            line = in.readLine();
            int target = Integer.parseInt(line);

            int ret = search(nums, target);

            String out = String.valueOf(ret);

            System.out.print(out);
        }
    }

}
