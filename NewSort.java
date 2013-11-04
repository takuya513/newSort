package newSort;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import quickSort.QuickSort;
import tools.MyArrayUtil;

public class NewSort <E extends Comparable> extends QuickSort<E>{
	ExecutorService executor;
	int threadsNum,arrayLength,pos,first;
	List<Callable<Object>> workers;
	LinkedList<MergeInfo> works;
	boolean endArray = false;
	boolean firstSet = false;


	public NewSort() {
		threadsNum = Runtime.getRuntime().availableProcessors();

	}


	public void sort(E[] array){
		this.array = array;
		arrayLength = array.length;
		executor = Executors.newCachedThreadPool();
		workers = new ArrayList<Callable<Object>>(threadsNum);
		works = new LinkedList<MergeInfo>();
		pos = 0;


		while(true){
			if(array[pos].compareTo(array[pos+1]) <= 0)
				ascendingOrder();
			else
				descendindOrder();

			pos++;
			if(endArray == true)
				break;
			else if(pos+1 > arrayLength-1){
				works.offer(new MergeInfo(pos,pos));
				break;
			}
		}
		//修正、適宜実行できるようにする
		try {
			executor.invokeAll(workers);

			//マージするメソッド
			parallelMergeSort();

			executor.invokeAll(workers);
		}catch (InterruptedException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}
		executor.shutdown();

	}

	//昇順メソッド
	public void ascendingOrder(){
		first = pos;
		while(array[pos].compareTo(array[pos+1]) <= 0){
			if(pos+1 >= arrayLength-1){
				works.offer(new MergeInfo(first,pos+1));
				endArray = true;
				return;
			}
			pos++;
		}
		works.offer(new MergeInfo(first,pos));
		return;
	}

	//降順メソッド
	public void descendindOrder(){
			first = pos;

		while(array[pos].compareTo(array[pos+1]) > 0){
			if(pos+1 >= arrayLength-1){
				workers.add(Executors.callable(new QuickSortWorker(first,pos+1)));
				works.offer(new MergeInfo(first,pos+1));
				endArray = true;
				return;
			}
			pos++;
		}

		workers.add(Executors.callable(new QuickSortWorker(first,pos)));//修正、もっと効率よくしたい
		works.offer(new MergeInfo(first,pos));
		return;
	}

	//マージする部分
	public void parallelMergeSort(){
		int workSize;
		MergeInfo info1,info2;
		System.out.println("workSize : "+works.size());
		while(true){
			workSize = works.size();
			while(workSize > 1){
				//二つのworksを取り出し、left,rightを取り出す
				info1 = works.remove();
				info2 = works.remove();

				workers.add(Executors.callable(new MergeSortWorker(info1.left,info1.right,info2.right)));
				works.offer(new MergeInfo(info1.left,info2.right));

				workSize = workSize-2;
				//一つ余りが出たら後ろに回す
				if(workSize == 1){
					MergeInfo tmp = works.remove();
					works.offer(tmp);
					break;
				}
			}


			try {
				executor.invokeAll(workers);
			} catch (InterruptedException e) {
				// TODO 自動生成された catch ブロック
				e.printStackTrace();
			}

			if(works.size() == 2){  //範囲の情報が二つのときは最後にマージして終了
				info1 = works.remove();
				info2 = works.remove();
				merge(info1.left,info1.right,info2.right,new LinkedList<E>());
				break;
			}else if(works.size() == 1)  //範囲の情報が一つのときは完成しているので終了
				break;
		}
	}

	public synchronized void merge(int left,int mid,int right,LinkedList<E> buff){
		int i = left,j = mid + 1;

		while(i <= mid && j <= right) {

			if(array[i].compareTo(array[j]) < 0){
				buff.add(array[i]); i++;
			}else{
				buff.add(array[j]); j++;
			}
		}


		while(i <= mid) { buff.add(array[i]);i++;}
		while(j <= right) { buff.add(array[j]);	j++;}
		for(i = left;i <= right; i++){ array[i] = buff.poll();}
	}


	class MergeSortWorker implements Runnable{
		private int left,right,mid;
		private LinkedList<E> buff;
		public MergeSortWorker(int left,int right){
			this.left = left;
			this.right = right;
			mid = (left + right) / 2;
			buff = new LinkedList<E>();
		}

		public MergeSortWorker(int left,int mid,int right){
			this.left = left;
			this.right = right;
			this.mid = mid;
			buff = new LinkedList<E>();
		}
		public void run(){
			merge(left,mid,right,buff);
		}
	}

	class MergeInfo{
		private int left,mid,right;
		MergeInfo(int left,int right){
			this.left = left;
			this.right = right;
		}

	}

	class QuickSortWorker implements Runnable {
		int left,right;
		public QuickSortWorker(int left,int right){
			this.left = left;
			this.right = right;
		}

		public void run() {
			quickSort(left,right);
		}
	}
}
