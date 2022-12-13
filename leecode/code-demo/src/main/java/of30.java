import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * @author : yangc
 * @date :2022/11/14 19:00
 * @description :
 * @modyified By:
 * <p>
 * 定义栈的数据结构，请在该类型中实现一个能够得到栈的最小元素的 min 函数在该栈中，调用 min、push 及 pop 的时间复杂度都是 O(1)。
 * <p>
 *  
 * <p>
 * 示例:
 * <p>
 * MinStack minStack = new MinStack();
 * minStack.push(-2);
 * minStack.push(0);
 * minStack.push(-3);
 * minStack.min();   --> 返回 -3.
 * minStack.pop();
 * minStack.top();      --> 返回 0.
 * minStack.min();   --> 返回 -2.
 *  
 * <p>
 * 提示：
 * <p>
 * 各函数的调用总次数不超过 20000 次
 * <p>
 * 来源：力扣（LeetCode）
 * 链接：https://leetcode.cn/problems/bao-han-minhan-shu-de-zhan-lcof
 * 著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
 */
public class of30 {
    Stack<Integer> stack1 = new Stack<>();
    Stack<Integer> stack2 = new Stack<>();

    public void push(int x) {
        stack1.push(x);
        if (stack2.isEmpty()) {
            stack2.push(x);
        } else {
            Integer peek = stack2.peek();
            stack2.push(x < peek ? x : peek);
        }
    }

    public void pop() {
        stack1.pop();
        stack2.pop();
    }

    public int top() {
        return stack1.peek();
    }

    public int min() {
        return stack2.peek();
    }


    public static void main(String[] args) {
        of30 of30 = new of30();
        of30.push(-2);
        of30.push(0);
        of30.push(-3);
        of30.pop();
        System.out.println(of30.min());
        System.out.println(of30.top());
        System.out.println(of30.min());
    }

}
