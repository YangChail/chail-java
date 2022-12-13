import java.util.Stack;

/**
 * @author : yangc
 * @date :2022/11/14 17:46
 * @description :
 * @modyified By:
 *
 * 用两个栈实现一个队列。队列的声明如下，请实现它的两个函数 appendTail 和 deleteHead ，
 * 分别完成在队列尾部插入整数和在队列头部删除整数的功能。(若队列中没有元素，deleteHead 操作返回 -1 )
 *
 *  
 *
 * 示例 1：
 *
 * 输入：
 * ["CQueue","appendTail","deleteHead","deleteHead","deleteHead"]
 * [[],[3],[],[],[]]
 * 输出：[null,null,3,-1,-1]
 * 示例 2：
 *
 * 输入：
 * ["CQueue","deleteHead","appendTail","appendTail","deleteHead","deleteHead"]
 * [[],[],[5],[2],[],[]]
 * 输出：[null,-1,null,null,5,2]
 * 提示：
 *
 * 1 <= values <= 10000
 * 最多会对 appendTail、deleteHead 进行 10000 次调用
 *
 * 来源：力扣（LeetCode）
 * 链接：https://leetcode.cn/problems/yong-liang-ge-zhan-shi-xian-dui-lie-lcof
 * 著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
 *
 *
 */
public class of09 {

    public static void main(String[] args) {
        of09 of09=new of09();
        of09.deleteHead();
        of09.appendTail(5);
        of09.appendTail(2);
        System.out.println( of09.deleteHead());
        System.out.println( of09.deleteHead());
    }


    Stack<Integer> A=new Stack<>();

    Stack<Integer> B=new Stack<>();

    public void appendTail(int value) {
        A.add(value);
    }

    public int deleteHead() {
        int res=-1;
        if(A.size()>0){
            while (!A.isEmpty()){
                B.add(A.pop());
            }
            res=B.pop();
            while (!B.isEmpty()){
                A.add(B.pop());
            }
        }
        return res;
    }

/**
 * Your CQueue object will be instantiated and called as such:
 * CQueue obj = new CQueue();
 * obj.appendTail(value);
 * int param_2 = obj.deleteHead();
 */



}
